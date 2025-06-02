from confluent_kafka import Producer
import ccxt
import time
import requests
import json
from datetime import datetime

# UZUPEŁNIJ: Nazwa topiku
TOPIC = 'mytopic3'

# Konfiguracja Kafki
conf = {'bootstrap.servers': 'broker:9092'}
producer = Producer(conf)

exchange = ccxt.kucoin()
symbols = ['BTC/USDT', 'ETH/USDT', 'ADA/USDT', 'SOL/USDT', 'LTC/USDT', 'DOT/USDT', 'XRP/USDT']

def fetch_current_prices(symbols):
    data = {}
    for symbol in symbols:
        try:
            ticker = exchange.fetch_ticker(symbol)
            data[symbol] = ticker['last']
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")
    data['timestamp'] = datetime.utcnow().isoformat()
    return data

def fetch_bitcoin_dominance():
    url = "https://api.coingecko.com/api/v3/global"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        jd = r.json()
        return jd['data']['market_cap_percentage']['btc']
    except Exception as e:
        print(f"Error fetching BTC Dominance: {e}")
        return None

while True:
    try:
        prices = fetch_current_prices(symbols)
        btc_dominance = fetch_bitcoin_dominance()
        if btc_dominance is not None:
            prices['btc_dominance'] = btc_dominance
        producer.produce(TOPIC, key="crypto_data", value=json.dumps(prices).encode('utf-8'))
        producer.flush()
        print(f"Dane wysłane do Kafka (topic: {TOPIC}): {prices}")
        time.sleep(5)

    except Exception as e:
        print(f"Błąd w producerze: {e}")
        time.sleep(5)