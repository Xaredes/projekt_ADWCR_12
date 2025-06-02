from kafka import KafkaConsumer
import json
import pandas as pd

# Wykrywanie altseason
def detect_altseason(df):
    btc_return = df.get('BTC/USDT', 0)
    altcoins = [col for col in df if col != 'BTC/USDT' and 'USDT' in col]
    alt_returns = sum(df[c] for c in altcoins) / len(altcoins)
    return (alt_returns - btc_return) > 0.01

# Generowanie rekomendacji
def recommend(is_altseason, df):
    signals = {}
    for coin, return_value in df.items():
        if is_altseason:
            signals[coin] = 'BUY' if return_value > 0.01 else 'HOLD'
        else:
            signals[coin] = 'SELL'
    return signals

# Połączenie z brokerem Kafka (zmiana na poprawny IP)
consumer = KafkaConsumer(
    'crypto-prices2',
    bootstrap_servers='172.18.0.28:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True
)

data_log = []

for message in consumer:
    record = message.value
    print(f"📥 Received from Kafka: {record}")
    data_log.append(record)

    if len(data_log) < 10:
        continue

    df = pd.DataFrame(data_log)
    df.set_index('timestamp', inplace=True)
    pct_returns = df.pct_change().iloc[-1].dropna().to_dict()

    is_alt = detect_altseason(pct_returns)
    signal = recommend(is_alt, pct_returns)

    print(f"🚨 Altseason Detected: {is_alt}")
    print(f"📊 Trading Signals: {signal}\n")
