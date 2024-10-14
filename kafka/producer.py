import logging
from kafka import KafkaProducer
import requests
import json
import time
from config import kafka_config, api_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STOCK_SYMBOL = 'AAPL'
KAFKA_TOPIC = kafka_config.KAFKA_TOPIC

producer = KafkaProducer(
    bootstrap_servers=[kafka_config.KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def send_stock_data(data):
    try:
        logger.info(f"Sending data to Kafka: {data}")
        producer.send(KAFKA_TOPIC, data)
        producer.flush()
    except Exception as e:
        logger.error(f"Error sending data to Kafka: {e}")


def fetch_stock_data():
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={STOCK_SYMBOL}&interval=1min&apikey={api_config.ALPHA_VANTAGE_API_KEY}'
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if 'Time Series (1min)' in data:
            for timestamp, values in data['Time Series (1min)'].items():
                stock_data = {
                    'symbol': STOCK_SYMBOL,
                    'price': values['1. open'],
                    'timestamp': timestamp
                }
                logger.info(f"Fetched stock data: {stock_data}")
                send_stock_data(stock_data)
    except requests.RequestException as e:
        logger.error(f"Error fetching data from Alpha Vantage: {e}")


if __name__ == '__main__':
    while True:
        fetch_stock_data()
        time.sleep(10)
