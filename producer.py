from kafka import KafkaProducer
import json
import time
import ccxt
from datetime import datetime

# Inicjalizacja giełdy i lista kryptowalut
exchange = ccxt.kucoin()
symbols = ['ETH/USDT', 'ADA/USDT', 'SOL/USDT', 'BTC/USDT']

def fetch_current_prices():
    data = {}
    for symbol in symbols:
        try:
            ticker = exchange.fetch_ticker(symbol)
            data[symbol] = ticker['last']
        except Exception as e:
            print(f"❌ Error fetching {symbol}: {e}")
    data['timestamp'] = datetime.utcnow().isoformat()
    return data

# Połączenie z brokerem Kafka (zmiana na poprawny IP)
producer = KafkaProducer(
    bootstrap_servers='172.18.0.28:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'crypto-prices2'

# Wysyłanie danych co 5 sekund
while True:
    price_data = fetch_current_prices()
    producer.send(topic, price_data)
    print(f"✅ Sent to Kafka: {price_data}")
    time.sleep(5)
