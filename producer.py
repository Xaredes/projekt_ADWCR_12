from confluent_kafka import Producer
import ccxt
import time
import requests
import json
from datetime import datetime
import logging

# --- Konfiguracja ---
TOPIC = 'mytopic11'  # Zmień na nazwę swojego topiku Kafka
BOOTSTRAP_SERVERS = 'broker:9092'  # Adres brokera Kafka
EXCHANGE = 'kucoin'  # Wybierz giełdę (ccxt)
SYMBOLS = [
    'BTC/USDT',
    'ETH/USDT',
    'ADA/USDT',
    'SOL/USDT',
    'LTC/USDT',
    'DOT/USDT',
    'XRP/USDT'
]

# --- Inicjalizacja ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
conf = {'bootstrap.servers': BOOTSTRAP_SERVERS}
producer = Producer(conf)
exchange = getattr(ccxt, EXCHANGE)()


# --- Funkcje ---
def fetch_current_prices(symbols_list):
    """Pobiera aktualne ceny z giełdy."""
    data = {}
    for s in symbols_list:
        try:
            ticker = exchange.fetch_ticker(s)
            data[s] = ticker['last']
        except Exception as e:
            logging.error(f"Błąd pobierania {s}: {e}")
    data['timestamp'] = datetime.utcnow().isoformat()
    return data


def fetch_bitcoin_dominance():
    """Pobiera dane o dominacji Bitcoina z CoinGecko."""
    url = "https://api.coingecko.com/api/v3/global"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        jd = r.json()
        return jd['data']['market_cap_percentage']['btc']
    except Exception as e:
        logging.error(f"Błąd pobierania dominacji BTC: {e}")
        return None


# --- Główna pętla ---
while True:
    try:
        prices = fetch_current_prices(SYMBOLS)
        btc_dominance = fetch_bitcoin_dominance()
        if btc_dominance is not None:
            prices['btc_dominance'] = btc_dominance
        producer.produce(TOPIC,
                         key="crypto_data",
                         value=json.dumps(prices).encode('utf-8'))
        producer.flush()
        logging.info(f"Wysłano do Kafka (topic: {TOPIC}): {prices}")
        time.sleep(5)

    except Exception as e:
        logging.error(f"Błąd w producerze: {e}")
        time.sleep(5)