# kafka_consumer.py
# Konsument Kafka - odbiera dane, analizuje altseason i generuje sygnały

import json
import pandas as pd
import numpy as np
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from collections import deque
import matplotlib.pyplot as plt

class AltseasonAnalyzer:
    def __init__(self, kafka_servers=['localhost:9092'], topic='crypto-data'):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda m: m.decode('utf-8') if m else None,
            auto_offset_reset='latest',
            group_id='altseason-analyzer'
        )
        
        # Buffer dla danych historycznych (ostatnie N próbek)
        self.data_buffer = deque(maxlen=100)
        
        # Portfolio state
        self.portfolio = {
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
        
        self.symbols = ['BTC/USDT', 'ETH/USDT', 'ADA/USDT', 'SOL/USDT', 
                       'LTC/USDT', 'DOT/USDT', 'XRP/USDT']
    
    def detect_altseason(self, returns_data, btc_dominance):
        """Detekcja altseason na podstawie zwrotów i dominacji BTC"""
        if len(returns_data) < 2:
            return False
            
        # Oblicz zwroty z ostatnich dwóch próbek
        current_prices = returns_data[-1]
        previous_prices = returns_data[-2]
        
        returns = {}
        for symbol in self.symbols:
            if (symbol in current_prices and symbol in previous_prices 
                and current_prices[symbol] and previous_prices[symbol]):
                curr_price = current_prices[symbol].get('price', 0)
                prev_price = previous_prices[symbol].get('price', 0)
                if prev_price > 0:
                    returns[symbol] = (curr_price - prev_price) / prev_price
                else:
                    returns[symbol] = 0
            else:
                returns[symbol] = 0
        
        # Analiza zwrotów
        btc_return = returns.get('BTC/USDT', 0)
        altcoins = [k for k in returns.keys() if k != 'BTC/USDT']
        alt_returns = np.mean([returns[k] for k in altcoins])
        
        # Progi dominacji BTC
        dominance_thresholds = [56.0, 54.2, 52.8]
        passed_thresholds = [t for t in dominance_thresholds 
                           if btc_dominance is not None and btc_dominance < t]
        count_passed = len(passed_thresholds)
        
        # Diagnostyka
        print(f"BTC zwrot:          {btc_return:.2%}")
        print(f"Śr. zwrot ALT:      {alt_returns:.2%}")
        if btc_dominance:
            print(f"BTC Dominance:      {btc_dominance:.2f}%")
            print(f"Przebite progi:     {passed_thresholds} (łącznie: {count_passed})")
        
        # Warunek altseason
        altseason = (alt_returns - btc_return) > 0.01 and count_passed >= 1
        print(f"Altseason?:         {'TAK' if altseason else 'NIE'}")
        
        return altseason
    
    def generate_signals(self, is_altseason, current_prices):
        """Generowanie sygnałów tradingowych"""
        if len(self.data_buffer) < 2:
            return {}
            
        # Oblicz zwroty dla aktualnych danych
        previous_prices = self.data_buffer[-2]
        returns = {}
        
        for symbol in self.symbols:
            if (symbol in current_prices and symbol in previous_prices 
                and current_prices[symbol] and previous_prices[symbol]):
                curr_price = current_prices[symbol].get('price', 0)
                prev_price = previous_prices[symbol].get('price', 0)
                if prev_price > 0:
                    returns[symbol] = (curr_price - prev_price) / prev_price
                else:
                    returns[symbol] = 0
            else:
                returns[symbol] = 0
        
        # Generuj sygnały
        signals = {}
        already_holding = list(self.portfolio['positions'].keys())
        
        for symbol in self.symbols:
            return_val = returns.get(symbol, 0)
            if is_altseason:
                signals[symbol] = 'BUY' if return_val > 0.01 else 'HOLD'
            else:
                signals[symbol] = 'HOLD' if symbol in already_holding else 'SELL'
        
        return signals
    
    def update_portfolio(self, signals, current_prices):
        """Aktualizacja stanu portfela na podstawie sygnałów"""
        # Lista monet do kupna
        coins_to_buy = [c for c, s in signals.items() if s == 'BUY']
        n_buy = len(coins_to_buy)
        cash_per_coin = (self.portfolio['cash'] / n_buy) if n_buy > 0 else 0
        
        for coin, signal in signals.items():
            if coin not in current_prices or not current_prices[coin]:
                continue
                
            price = current_prices[coin].get('price', 0)
            if price <= 0:
                continue
            
            if signal == 'BUY' and n_buy > 0 and self.portfolio['cash'] >= cash_per_coin:
                qty_to_buy = cash_per_coin / price
                
                if coin in self.portfolio['positions']:
                    # Zwiększ istniejącą pozycję
                    existing = self.portfolio['positions'][coin]
                    prev_qty = existing['qty']
                    prev_entry = existing['entry_price']
                    prev_alloc = existing['allocated']
                    
                    new_alloc = prev_alloc + cash_per_coin
                    total_qty = prev_qty + qty_to_buy
                    new_entry_price = (prev_entry * prev_qty + price * qty_to_buy) / total_qty
                    
                    self.portfolio['positions'][coin] = {
                        'qty': total_qty,
                        'entry_price': new_entry_price,
                        'allocated': new_alloc
                    }
                else:
                    # Nowa pozycja
                    self.portfolio['positions'][coin] = {
                        'qty': qty_to_buy,
                        'entry_price': price,
                        'allocated': cash_per_coin
                    }
                
                self.portfolio['cash'] -= cash_per_coin
            
            elif signal == 'SELL' and coin in self.portfolio['positions']:
                # Sprzedaj całą pozycję
                qty_held = self.portfolio['positions'][coin]['qty']
                self.portfolio['cash'] += qty_held * price
                del self.portfolio['positions'][coin]
        
        # Oblicz całkowitą wartość portfela
        total_value = self.portfolio['cash']
        for coin, pos in self.portfolio['positions'].items():
            if coin in current_prices and current_prices[coin]:
                market_price = current_prices[coin].get('price', 0)
                total_value += pos['qty'] * market_price
        
        # Zapisz do historii
        self.portfolio['history'].append({
            'timestamp': datetime.utcnow(),
            'value': total_value
        })
        
        # Oblicz % zmianę
        initial = self.portfolio.get('initial_value', 10000)
        pct_change = (total_value - initial) / initial * 100
        
        return total_value, pct_change
    
    def print_portfolio_status(self, current_prices, total_value, pct_change):
        """Wyświetl status portfela"""
        print("\n=== STAN PORTFELA ===")
        print(f"Cash: {self.portfolio['cash']:.2f} USDT")
        
        if self.portfolio['positions']:
            print("Pozycje:")
            for coin, pos in self.portfolio['positions'].items():
                if coin in current_prices and current_prices[coin]:
                    market_price = current_prices[coin].get('price', 0)
                    market_val = pos['qty'] * market_price
                    alloc_pct = (market_val / total_value * 100) if total_value > 0 else 0
                    profit_loss = ((market_price - pos['entry_price']) / pos['entry_price'] * 100) if pos['entry_price'] > 0 else 0
                    
                    print(f"  {coin}: {pos['qty']:.6f} @ ${pos['entry_price']:.2f}")
                    print(f"    Wartość: ${market_val:.2f} ({alloc_pct:.1f}%) | P/L: {profit_loss:+.1f}%")
        
        print(f"Całkowita wartość: ${total_value:.2f} USDT")
        print(f"Zmiana od początku: {pct_change:+.2f}%")
        print("=" * 50)
    
    def consume_and_analyze(self):
        """Główna pętla konsumenta"""
        print("Starting Altseason Analyzer Consumer...")
        print("Waiting for messages...")
        
        for message in self.consumer:
            try:
                data = message.value
                
                if data.get('type') != 'market_data':
                    continue
                
                timestamp = data.get('timestamp')
                prices_data = data.get('prices', {})
                btc_dominance = data.get('btc_dominance')
                
                # Dodaj do buffera
                self.data_buffer.append(prices_data)
                
                print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Otrzymano dane z {timestamp}")
                
                # Wyświetl aktualne ceny
                print("Aktualne ceny:")
                for symbol in self.symbols:
                    if symbol in prices_data and prices_data[symbol]:
                        price = prices_data[symbol].get('price', 0)
                        change = prices_data[symbol].get('change_24h', 0)
                        print(f"  {symbol}: ${price:.4f} ({change:+.2f}%)")
                
                # Analiza altseason (potrzebujemy co najmniej 2 próbki)
                if len(self.data_buffer) >= 2:
                    print(f"\nBTC Dominance: {btc_dominance:.2f}%" if btc_dominance else "BTC Dominance: N/A")
                    
                    # Detekcja altseason
                    is_altseason = self.detect_altseason(list(self.data_buffer), btc_dominance)
                    
                    # Generowanie sygnałów
                    signals = self.generate_signals(is_altseason, prices_data)
                    print(f"\nSygnały: {signals}")
                    
                    # Aktualizacja portfela
                    total_val, pct_change = self.update_portfolio(signals, prices_data)
                    
                    # Status portfela
                    self.print_portfolio_status(prices_data, total_val, pct_change)
                
                else:
                    print("Zbieranie danych historycznych... (potrzeba co najmniej 2 próbek)")
                
            except Exception as e:
                print(f"Error processing message: {e}")
    
    def close(self):
        """Zamknij konsumenta"""
        self.consumer.close()

if __name__ == "__main__":
    # Konfiguracja
    KAFKA_SERVERS = ['localhost:9092']  # Dostosuj do środowiska uczelni
    TOPIC_NAME = 'crypto-altseason-data'
    
    analyzer = AltseasonAnalyzer(kafka_servers=KAFKA_SERVERS, topic=TOPIC_NAME)
    
    try:
        analyzer.consume_and_analyze()
    except KeyboardInterrupt:
        print("\nShutting down analyzer...")
        analyzer.close()
        print("Analyzer closed.")
