from confluent_kafka import Consumer
import json
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt

# Funkcje z Twojego Notebooka (detect_altseason, generate_signals, log_and_backtest_advanced, evaluate_performance)
# ... (wklej je tutaj)

# UZUPEŁNIJ: Nazwa topiku
TOPIC = 'mytopic3'

# Konfiguracja Kafki
conf = {
    'bootstrap.servers': 'broker:9092',
    'group.id': 'altseason_consumer_group',  # Możesz zmienić, ale musi być spójne
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe([TOPIC])

symbols = ['BTC/USDT', 'ETH/USDT', 'ADA/USDT', 'SOL/USDT', 'LTC/USDT', 'DOT/USDT', 'XRP/USDT']

data_log = []
portfolio = {
    'cash': 5635.0,
    'positions': {
        'BTC/USDT': {'qty': 0.05, 'entry_price': 28000.0, 'allocated': 1400.0},
        'ETH/USDT': {'qty': 0.50, 'entry_price': 1800.0, 'allocated': 900.0},
        'ADA/USDT': {'qty': 1000.0, 'entry_price': 0.40, 'allocated': 400.0},
        'SOL/USDT': {'qty': 20.0, 'entry_price': 22.00, 'allocated': 440.0},
        'LTC/USDT': {'qty': 10.0, 'entry_price': 100.00, 'allocated': 1000.0},
        'DOT/USDT': {'qty': 5.0, 'entry_price': 25.00, 'allocated': 125.0},
        'XRP/USDT': {'qty': 200.0, 'entry_price': 0.50, 'allocated': 100.0}
    },
    'initial_value': 10000.0,
    'history': []
}

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            print("Konsument: Odebrano None (timeout)")  # Dodane logowanie
            continue
        if msg.error():
            print(f"Konsument: Błąd konsumenta: {msg.error()}")
            continue

        if msg.value() is not None:
            raw_value = msg.value().decode('utf-8')
            print(f"Konsument: Odebrano z Kafka (topic: {TOPIC}): {raw_value}")  # Dodane logowanie
            try:
                data = json.loads(raw_value)
                # --- Twój oryginalny kod przetwarzania danych ---
                df_raw = pd.DataFrame(data_log)
                returns_df = df_raw[symbols].pct_change().dropna().reset_index(drop=True)
                returns_df['timestamp'] = df_raw['timestamp'].iloc[1:].reset_index(drop=True)

                if returns_df.empty:
                    print("Za mało danych, aby policzyć zwroty.\n")
                    continue

                last_returns_row = returns_df[symbols].iloc[[-1]].reset_index(drop=True)
                btc_dom = data.get('btc_dominance')
                is_alt = detect_altseason(last_returns_row, btc_dom)
                already_holding = list(portfolio['positions'].keys())
                signals = generate_signals(is_alt, last_returns_row, already_holding=already_holding)
                prices_now = {s: data[s] for s in symbols}
                portfolio = log_and_backtest_advanced(signals, prices_now, portfolio)
                # --- Koniec Twojego oryginalnego kodu ---
            except json.JSONDecodeError as e:
                print(f"Konsument: Błąd JSON: {e}, dla danych: {raw_value}")
                continue  # Przejdź do następnej iteracji pętli
        else:
            print("Konsument: Odebrano pustą wiadomość (msg.value() is None)")  # Dodane logowanie
            continue

except KeyboardInterrupt:
    print("Zatrzymano konsumenta.")
finally:
    consumer.close()