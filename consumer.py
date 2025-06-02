from confluent_kafka import Consumer, KafkaError
import json
import pandas as pd
import logging
from datetime import datetime
import matplotlib.pyplot as plt  # Dodajemy import matplotlib

# --- Konfiguracja ---
TOPIC = 'mytopic10'  # Zmień na nazwę swojego topiku Kafka
BOOTSTRAP_SERVERS = 'broker:9092'  # Adres brokera Kafka
GROUP_ID = 'my_consumer_group'  # Unikalna nazwa grupy konsumentów
SYMBOLS = [
    'BTC/USDT',
    'ETH/USDT',
    'ADA/USDT',
    'SOL/USDT',
    'LTC/USDT',
    'DOT/USDT',
    'XRP/USDT'
]
REQUIRED_KEYS = SYMBOLS + ['timestamp', 'btc_dominance']

# --- Inicjalizacja ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
conf = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest'  # Odczyt od początku, jeśli brak offsetu
}
consumer = Consumer(conf)
consumer.subscribe([TOPIC])

data_log_df = pd.DataFrame()  # Inicjalizacja pustego DataFrame

# --- Funkcje (wklej z Notebooka) ---
def detect_altseason(df_single_row, btc_dominance):
    """
    Oczekuje:
      - df_single_row: DataFrame z dokładnie jednym wierszem, kolumny to pary 'XXX/USDT'
      - btc_dominance: float, procentowy udział Bitcoina (np. 55.3)
    Zwraca:
      - altseason (bool)
    """
    # 1) Pobranie zwrotu BTC
    btc_return = df_single_row.get('BTC/USDT', pd.Series([0.0])).iloc[0]
    # 2) Lista altów: wszystkie pary USDT poza BTC
    altcoins = [col for col in df_single_row.columns if col.endswith('/USDT') and col != 'BTC/USDT']
    # 3) Średni zwrot altów
    alt_returns = df_single_row[altcoins].mean(axis=1).iloc[0]

    # 4) Progi BTC Dominance
    dominance_thresholds = [56.0, 54.2, 52.8]
    passed_thresholds = [t for t in dominance_thresholds if btc_dominance is not None and btc_dominance < t]
    count_passed = len(passed_thresholds)

    # 5) Informacje diagnostyczne
    print(f"BTC zwrot:          {btc_return:.2%}")
    print(f"Śr. zwrot ALT:      {alt_returns:.2%}")
    if btc_dominance is not None:
        print(f"BTC Dominance:      {btc_dominance:.2f}%")
        print(f"Przebite progi:     {passed_thresholds}  (łącznie: {count_passed} z {len(dominance_thresholds)})")
    else:
        print("BTC Dominance:      brak danych (błąd pobrania)")
        print("Przebite progi:     []  (nie można ocenić bez dominacji)")

    # 6) Warunek Altseason:
    #    - alt_returns – btc_return > 1pp (0.01)
    #    - co najmniej jeden próg dominacji jest przebity (count_passed >= 1)
    altseason = False
    if (alt_returns - btc_return) > 0.01 and count_passed >= 1:
        altseason = True

    print(f"Altseason?:         {'TAK' if altseason else 'NIE'}\n")
    return altseason

def generate_signals(is_alt_flag, df_single_row, already_holding=None):
    """
    Na podstawie flagi altseason i df_single_row generuje sygnały. Dodatkowo:
    - already_holding: lista symboli (np. ['BTC/USDT', 'ETH/USDT', ...])
      wskazująca, które monety są już w portfelu (pochodzą np. z portfolio['positions'].keys()).
    Reguły:
      - jeśli altseason=True i zwrot > 1%   => BUY
      - jeśli altseason=True i zwrot ≤ 1%    => HOLD
      - jeśli altseason=False                => SELL (ale nie sprzedawaj, jeśli coin jest w already_holding)
    Zwraca słownik { 'BTC/USDT': 'SELL', 'ETH/USDT': 'BUY', ... }
    """
    if already_holding is None:
        already_holding = []

    signals_out = {}
    for c in df_single_row.columns:
        rv = df_single_row[c].iloc[0]
        if is_alt_flag:
            signals_out[c] = 'BUY' if rv > 0.01 else 'HOLD'
        else:
            # jeżeli próbujemy sprzedać coś, czego już mamy, to zamieniamy SELL na HOLD
            if c in already_holding:
                signals_out[c] = 'HOLD'
            else:
                signals_out[c] = 'SELL'
    return signals_out

def log_and_backtest_advanced(signals_dict, prices_dict, portfolio_state):
    """
    - signals_dict: {'BTC/USDT': 'SELL', 'ETH/USDT': 'BUY', ...}
    - prices_dict: {'BTC/USDT': 28700, 'ETH/USDT': 1840, ...}
    portfolio_state struktura:
      {
        'cash': float,
        'positions': {
            'ETH/USDT': {'qty': float, 'entry_price': float, 'allocated': float},
            ...
        },
        'initial_value': float,  # wartość początkowa portfela
        'history': [ {'timestamp': dt, 'value': float}, ... ]
      }
    Funkcja:
      - BUY: dzieli cash po równo między waluty, które mają sygnał BUY,
             przy zakupie aktualizuje entry_price jako ważoną średnią oraz sumę alokacji.
      - SELL: sprzedaje całą pozycję i usuwa z 'positions'.
      - HOLD: utrzymuje obecną pozycję.
      - Na koniec liczy wartość portfela, zapisuje do historii i wypisuje % zmiany od wartości początkowej.
    """
    # 1. Lista monet do BUY
    coins_buy = [c for c, s in signals_dict.items() if s == 'BUY']
    n_buy = len(coins_buy)
    cash_avail = portfolio_state['cash']
    cash_per_coin = (cash_avail / n_buy) if n_buy > 0 else 0

    for coin, sig in signals_dict.items():
        price = prices_dict.get(coin, None)
        if price is None:
            continue

        if sig == 'BUY':
            if portfolio_state['cash'] >= cash_per_coin and n_buy > 0:
                qty_to_buy = cash_per_coin / price
                if coin in portfolio_state['positions']:
                    existing = portfolio_state['positions'][coin]
                    prev_qty = existing['qty']
                    prev_entry = existing['entry_price']
                    prev_alloc = existing['allocated']
                    new_alloc = prev_alloc + cash_per_coin
                    total_qty = prev_qty + qty_to_buy
                    new_entry_price = (prev_entry * prev_qty + price * qty_to_buy) / total_qty
                    portfolio_state['positions'][coin] = {
                        'qty': total_qty,
                        'entry_price': new_entry_price,
                        'allocated': new_alloc
                    }
                else:
                    portfolio_state['positions'][coin] = {
                        'qty': qty_to_buy,
                        'entry_price': price,
                        'allocated': cash_per_coin
                    }
                portfolio_state['cash'] -= cash_per_coin

        elif sig == 'SELL':
            if coin in portfolio_state['positions']:
                qty_held = portfolio_state['positions'][coin]['qty']
                portfolio_state['cash'] += qty_held * price
                del portfolio_state['positions'][coin]
        # HOLD: nie zmieniaj stanu

    # 2. Obliczamy wartość portfela: cash + suma(market_value każdej pozycji)
    total_val = portfolio_state['cash']
    for c_sym, pos in portfolio_state['positions'].items():
        if c_sym in prices_dict:
            total_val += pos['qty'] * prices_dict.get(c_sym, 0)

    # 3. Zapis do historii
    portfolio_state['history'].append({
        'timestamp': datetime.utcnow(),
        'value': total_val
    })

    # 4. Obliczamy procentową zmianę względem wartości początkowej
    initial = portfolio_state.get('initial_value', None)
    if initial is not None and initial > 0:
        pct_change = (total_val - initial) / initial * 100
    else:
        pct_change = 0.0

    # 5. Wypisujemy stan portfela wraz z detalami pozycji i % zmiany
    print("Stan portfela (advanced backtest):")
    print(f"  Cash: {portfolio_state['cash']:.2f} USDT")
    if portfolio_state['positions']:
        print("  Pozycje:")
        for c_sym, pos in portfolio_state['positions'].items():
            market_val = pos['qty'] * prices_dict.get(c_sym, 0)
            alloc_pct = (market_val / total_val * 100) if total_val > 0 else 0
            print(f"    {c_sym}: qty={pos['qty']:.6f}, entry_price={pos['entry_price']:.2f}, "
                  f"market_val={market_val:.2f} USDT, allocated={pos['allocated']:.2f} USDT, "
                  f"allocation={alloc_pct:.2f}%")
    else:
        print("  Brak otwartych pozycji.")
    print(f"  Wartość portfela: {total_val:.2f} USDT")
    print(f"  % zmiana od wartości początkowej: {pct_change:+.2f}%\n")

    return portfolio_state

def evaluate_performance(history_list):
    """
    - history_list: lista słowników [{'timestamp': dt, 'value': float}, ...]
    Buduje DataFrame, liczy zwroty i Sharpe Ratio, rysuje wykres wartości portfela.
    Zwraca (DataFrame, sharpe).
    """
    dfh = pd.DataFrame(history_list)
    dfh.set_index('timestamp', inplace=True)
    dfh.sort_index(inplace=True)
    dfh['returns'] = dfh['value'].pct_change()

    # Przybliżony Sharpe (zakładamy 365 prób rocznie)
    sharpe = dfh['returns'].mean() / dfh['returns'].std() * (365 ** 0.5)

    plt.figure(figsize=(9, 5))
    plt.plot(dfh.index, dfh['value'], marker='o', linestyle='-')
    plt.title(f"Wartość portfela | Sharpe Ratio: {sharpe:.2f}")
    plt.xlabel("Czas")
    plt.ylabel("Wartość portfela (USDT)")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    return dfh, sharpe

# --- Inicjalizacja portfela ---
portfolio = {
    'cash': 5635.0,
    'positions': {
        'BTC/USDT': {
            'qty': 0.05,
            'entry_price': 28000.0,
            'allocated': 1400.0
        },
        'ETH/USDT': {
            'qty': 0.50,
            'entry_price': 1800.0,
            'allocated': 900.0
        },
        'ADA/USDT': {
            'qty': 1000.0,
            'entry_price': 0.40,
            'allocated': 400.0
        },
        'SOL/USDT': {
            'qty': 20.0,
            'entry_price': 22.00,
            'allocated': 440.0
        },
        'LTC/USDT': {
            'qty': 10.0,
            'entry_price': 100.00,
            'allocated': 1000.0
        },
        'DOT/USDT': {
            'qty': 5.0,
            'entry_price': 25.00,
            'allocated': 125.0
        },
        'XRP/USDT': {
            'qty': 200.0,
            'entry_price': 0.50,
            'allocated': 100.0
        }
    },
    'initial_value': 10000.0,
    'history': []
}

try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                logging.info("Koniec partycji, oczekiwanie na nowe wiadomości")
                continue
            else:
                logging.error(f"Błąd Kafka: {msg.error()}")
                break

        if msg.value() is not None:
            raw_value = msg.value().decode('utf-8')
            logging.info(f"Odebrano z Kafka (topic: {TOPIC}): {raw_value}")
            try:
                data = json.loads(raw_value)

                # --- Sprawdzanie i uzupełnianie kluczy ---
                if not all(key in data for key in REQUIRED_KEYS):
                    logging.error(f"Brak wymaganych kluczy w danych: {raw_value}")
                    for key in REQUIRED_KEYS:
                        if key not in data:
                            data[key] = None

                # --- Tworzenie DataFrame z JEDNEJ odebranej wiadomości ---
                new_row_df = pd.DataFrame([data])

                # --- Deklaracja globalna (NA SAMYM POCZĄTKU pętli) ---
                global data_log_df

                # --- Dołączanie nowego wiersza do istniejącego DataFrame ---
                if data_log_df.empty:
                    data_log_df = new_row_df.copy()  # Pierwszy wiersz
                else:
                    data_log_df = pd.concat([data_log_df, new_row_df], ignore_index=True)

                # --- Przetwarzanie danych ---
                try:
                    if len(data_log_df) > 1:
                        returns_df = data_log_df[SYMBOLS].pct_change().dropna().reset_index(drop=True)
                        if not returns_df.empty:  # Dodana kontrola
                            returns_df['timestamp'] = data_log_df['timestamp'].iloc[1:].reset_index(drop=True)
                            last_returns_row = returns_df[SYMBOLS].iloc[[-1]].reset_index(drop=True)
                            btc_dom = data_log_df['btc_dominance'].iloc[-1]
                            is_alt = detect_altseason(last_returns_row, btc_dom)
                            already_holding = list(portfolio['positions'].keys())
                            signals = generate_signals(is_alt,
                                                     last_returns_row,
                                                     already_holding=already_holding)
                            prices_now = {
                                s: data_log_df[s].iloc[-1]
                                for s in SYMBOLS
                            }
                            portfolio = log_and_backtest_advanced(
                                signals, prices_now, portfolio)
                        else:
                            logging.warning("Brak wystarczających danych do analizy zwrotów.")
                    else:
                        logging.info("Odebrano pierwszą wiadomość, czekam na więcej danych.")

                except KeyError as e:
                    logging.error(
                        f"Błąd KeyError podczas przetwarzania DataFrame: {e}")
                except Exception as e:
                    logging.error(f"Inny błąd DataFrame: {e}")

            except json.JSONDecodeError as e:
                logging.error(f"Błąd JSON: {e}, dla danych: {raw_value}")
                continue
        # --- Koniec pętli while ---

except KeyboardInterrupt:
    print("Zatrzymano konsumenta.")
finally:
    consumer.close()