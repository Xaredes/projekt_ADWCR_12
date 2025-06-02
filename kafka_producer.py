# kafka_producer.py
# Producent Kafka - pobiera dane kryptowalut i wysyła do topicu

import json
import time
import ccxt
import requests
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

class CryptoDataProducer:
    def __init__(self, kafka_servers=['localhost:9092'], topic='crypto-data'):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None
        )
        self.topic = topic
        self.exchange = ccxt.kucoin()
        self.symbols = [
            'BTC/USDT', 'ETH/USDT', 'ADA/USDT', 'SOL/USDT',
            'LTC/USDT', 'DOT/USDT', 'XRP/USDT'
        ]
        
    def fetch_current_prices(self):
        """Pobiera aktualne ceny kryptowalut"""
        data = {}
        for symbol in self.symbols:
            try:
                ticker = self.exchange.fetch_ticker(symbol)
                data[symbol] = {
                    'price': ticker['last'],
                    'volume': ticker['baseVolume'],
                    'change_24h': ticker['percentage']
                }
            except Exception as e:
                print(f"Error fetching {symbol}: {e}")
                data[symbol] = None
        
        data['timestamp'] = datetime.utcnow().isoformat()
        return data
    
    def fetch_bitcoin_dominance(self):
        """Pobiera dominację Bitcoina"""
        url = "https://api.coingecko.com/api/v3/global"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            return data['data']['market_cap_percentage']['btc']
        except Exception as e:
            print(f"Error fetching BTC Dominance: {e}")
            return None
    
    def produce_data(self, interval=5):
        """Główna pętla producenta - wysyła dane co określony interwał"""
        print(f"Starting crypto data producer... Sending to topic: {self.topic}")
        
        while True:
            try:
                # Pobierz dane cenowe
                price_data = self.fetch_current_prices()
                
                # Pobierz dominację BTC
                btc_dominance = self.fetch_bitcoin_dominance()
                
                # Przygotuj kompletny pakiet danych
                message = {
                    'type': 'market_data',
                    'timestamp': datetime.utcnow().isoformat(),
                    'prices': price_data,
                    'btc_dominance': btc_dominance
                }
                
                # Wyślij do Kafka
                future = self.producer.send(
                    self.topic,
                    key='market_update',
                    value=message
                )
                
                # Czekaj na potwierdzenie
                record_metadata = future.get(timeout=10)
                
                print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] Sent data to {record_metadata.topic}:"
                      f"partition {record_metadata.partition}, offset {record_metadata.offset}")
                
                # Wyświetl kluczowe dane
                print(f"  BTC: ${price_data.get('BTC/USDT', {}).get('price', 'N/A')}")
                print(f"  ETH: ${price_data.get('ETH/USDT', {}).get('price', 'N/A')}")
                print(f"  BTC Dominance: {btc_dominance:.2f}%" if btc_dominance else "  BTC Dominance: N/A")
                print("  ---")
                
            except KafkaError as e:
                print(f"Kafka error: {e}")
            except Exception as e:
                print(f"General error: {e}")
            
            time.sleep(interval)
    
    def close(self):
        """Zamknij producenta"""
        self.producer.close()

if __name__ == "__main__":
    # Konfiguracja - dostosuj do swojego środowiska
    KAFKA_SERVERS = ['localhost:9092']  # Zmień na adres serwera Kafka w środowisku uczelni
    TOPIC_NAME = 'crypto-altseason-data'
    
    producer = CryptoDataProducer(kafka_servers=KAFKA_SERVERS, topic=TOPIC_NAME)
    
    try:
        producer.produce_data(interval=10)  # Co 10 sekund
    except KeyboardInterrupt:
        print("\nShutting down producer...")
        producer.close()
        print("Producer closed.")
