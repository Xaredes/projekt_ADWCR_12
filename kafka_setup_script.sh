#!/bin/bash
# setup_kafka.sh - Skrypt do konfiguracji i uruchomienia środowiska Kafka

echo "=== KONFIGURACJA KAFKA DLA PROJEKTU ALTSEASON ==="

# Kolory dla lepszej czytelności
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Konfiguracja
KAFKA_DIR="/opt/kafka"  # Dostosuj ścieżkę do instalacji Kafka w środowisku uczelni
TOPIC_NAME="crypto-altseason-data"
PARTITIONS=3
REPLICATION_FACTOR=1

echo -e "${YELLOW}Sprawdzanie statusu Kafka...${NC}"

# Funkcja sprawdzania czy Kafka działa
check_kafka() {
    if pgrep -f kafka > /dev/null; then
        echo -e "${GREEN}✓ Kafka jest uruchomiona${NC}"
        return 0
    else
        echo -e "${RED}✗ Kafka nie jest uruchomiona${NC}"
        return 1
    fi
}

# Funkcja tworzenia topicu
create_topic() {
    echo -e "${YELLOW}Tworzenie topicu: $TOPIC_NAME${NC}"
    
    $KAFKA_DIR/bin/kafka-topics.sh \
        --create \
        --topic $TOPIC_NAME \
        --bootstrap-server localhost:9092 \
        --partitions $PARTITIONS \
        --replication-factor $REPLICATION_FACTOR
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Topic utworzony pomyślnie${NC}"
    else
        echo -e "${YELLOW}Topic może już istnieć lub wystąpił błąd${NC}"
    fi
}

# Funkcja listowania topików
list_topics() {
    echo -e "${YELLOW}Lista dostępnych topików:${NC}"
    $KAFKA_DIR/bin/kafka-topics.sh \
        --list \
        --bootstrap-server localhost:9092
}

# Funkcja sprawdzania szczegółów topicu
describe_topic() {
    echo -e "${YELLOW}Szczegóły topicu $TOPIC_NAME:${NC}"
    $KAFKA_DIR/bin/kafka-topics.sh \
        --describe \
        --topic $TOPIC_NAME \
        --bootstrap-server localhost:9092
}

# Główna logika
echo "1. Sprawdzanie Kafka..."
if ! check_kafka; then
    echo -e "${RED}Uruchom najpierw Kafka:${NC}"
    echo "sudo systemctl start kafka"
    echo "lub"
    echo "$KAFKA_DIR/bin/kafka-server-start.sh $KAFKA_DIR/config/server.properties"
    exit 1
fi

echo "2. Tworzenie topicu..."
create_topic

echo "3. Sprawdzanie topików..."
list_topics

echo "4. Szczegóły topicu..."
describe_topic

echo -e "${GREEN}=== KONFIGURACJA ZAKOŃCZONA ===${NC}"
echo
echo -e "${YELLOW}Jak uruchomić projekt:${NC}"
echo "1. Terminal 1 (Producer):"
echo "   python kafka_producer.py"
echo
echo "2. Terminal 2 (Consumer):"
echo "   python kafka_consumer.py"
echo
echo "3. Opcjonalnie - Terminal 3 (Monitoring):"
echo "   $KAFKA_DIR/bin/kafka-console-consumer.sh \\"
echo "     --bootstrap-server localhost:9092 \\"
echo "     --topic $TOPIC_NAME \\"
echo "     --from-beginning"
echo
echo -e "${YELLOW}Przydatne komendy:${NC}"
echo "• Lista konsumentów: $KAFKA_DIR/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list"
echo "• Usunięcie topicu: $KAFKA_DIR/bin/kafka-topics.sh --delete --topic $TOPIC_NAME --bootstrap-server localhost:9092"
echo "• Logi Kafka: tail -f $KAFKA_DIR/logs/server.log"
