import logging
from confluent_kafka import Consumer
import json
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt

# --- Konfiguracja Logowania ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Konfiguracja Kafki ---
TOPIC = 'mytopic6'
conf = {
    'bootstrap.servers': 'broker:9092',
    'group.id': 'altseason_consumer_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe([TOPIC])

# --- Stałe ---
symbols = ['BTC/USDT', 'ETH/USDT', 'ADA/USDT', 'SOL/USDT', 'LTC/USDT', 'DOT/USDT', 'XRP/USDT']
required_keys = symbols + ['timestamp', 'btc_dominance']  # Wszystkie wymagane klucze

# --- Inicjalizacja ---
data_log = []
portfolio = { /* ... Twoje portfolio ... */ }

# --- Główna pętla ---
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            logging.debug("Konsument: Odebrano None (timeout)")
            continue
        if msg.error():
            logging.error(f"Konsument: Błąd konsumenta: {msg.error()}")
            continue

        if msg.value() is not None:
            raw_value = msg.value().decode('utf-8')
            logging.info(f"Konsument: Odebrano z Kafka (topic: {TOPIC}): {raw_value}")
            try:
                data = json.loads(raw_value)

                # --- Sprawdzanie i uzupełnianie kluczy ---
                if not all(key in data for key in required_keys):
                    logging.error(f"Konsument: Brak wymaganych kluczy w danych: {raw_value}")
                    # Uzupełnij brakujące klucze wartościami None lub NaN (lepsze dla Pandas)
                    for key in required_keys:
                        if key not in data:
                            data[key] = None  # Lub data[key] = float('nan')
                    # continue  # UWAGA: Usunięto continue, bo uzupełniamy dane!

                # --- Twój oryginalny kod przetwarzania danych ---
                df_raw = pd.DataFrame(data_log)
                try:
                    returns_df = df_raw[symbols].pct_change().dropna().reset_index(drop=True)
                    returns_df['timestamp'] = df_raw['timestamp'].iloc[1:].reset_index(drop=True)

                    if returns_df.empty:
                        logging.warning("Za mało danych, aby policzyć zwroty.\n")
                        continue

                    last_returns_row = returns_df[symbols].iloc[[-1]].reset_index(drop=True)
                    btc_dom = data.get('btc_dominance')  # To teraz bezpieczne, bo klucz istnieje
                    is_alt = detect_altseason(last_returns_row, btc_dom)
                    already_holding = list(portfolio['positions'].keys())
                    signals = generate_signals(is_alt, last_returns_row, already_holding=already_holding)
                    prices_now = {s: data[s] for s in symbols}
                    portfolio = log_and_backtest_advanced(signals, prices_now, portfolio)

                except KeyError as e:
                    logging.error(f"Błąd KeyError podczas przetwarzania DataFrame: {e}")
                except Exception as e:
                    logging.error(f"Inny błąd DataFrame: {e}")

                # --- Koniec Twojego oryginalnego kodu ---

            except json.JSONDecodeError as e:
                logging.error(f"Konsument: Błąd JSON: {e}, dla danych: {raw_value}")
                continue
        else:
            logging.debug("Konsument: Odebrano pustą wiadomość (msg.value() is None)")

except KeyboardInterrupt:
    print("Zatrzymano konsumenta.")
finally:
    consumer.close()