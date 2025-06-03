from confluent_kafka import Producer
import ccxt
import time
import requests
import json
from datetime import datetime

# Konfiguracja Kafki (dostosuj!)
conf = {'bootstrap.servers': 'adres_serwera_kafka:9092'}  # Zmień na adres Twojego brokera Kafka
producer = Producer(conf)
topic = 'mytopic30'  # Nazwa topiku

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
    data['timestamp'] = datetime.utcnow().isoformat()  # Ważne: Serializacja daty do stringa
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
        # Serializacja danych do JSON (Kafka operuje na bajtach)
        producer.produce(topic, key="crypto_data", value=json.dumps(prices).encode('utf-8'))
        producer.flush()  # Upewnij się, że wiadomości zostały wysłane
        print(f"Dane wysłane do Kafka: {prices}")
        time.sleep(5)

    except Exception as e:
        print(f"Błąd w producerze: {e}")
        time.sleep(5)