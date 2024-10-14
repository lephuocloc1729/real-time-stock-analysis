import logging
from kafka import KafkaConsumer
import psycopg2
import json
from config import kafka_config, db_config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

consumer = KafkaConsumer(
    kafka_config.KAFKA_TOPIC,
    bootstrap_servers=[kafka_config.KAFKA_BROKER],
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

try:
    conn = psycopg2.connect(
        host=db_config.DB_HOST,
        port=db_config.DB_PORT,
        dbname=db_config.DB_NAME,
        user=db_config.DB_USER,
        password=db_config.DB_PASSWORD
    )
    cursor = conn.cursor()

    for message in consumer:
        data = message.value
        try:
            cursor.execute(
                "INSERT INTO stocks (symbol, price, timestamp) VALUES (%s, %s, %s)",
                (data['symbol'], data['price'], data['timestamp'])
            )
            conn.commit()
            logger.info(f"Inserted data into database: {data}")
        except Exception as e:
            logger.error(f"Error inserting data into database: {e}")
except Exception as e:
    logger.error(
        f"Error consuming data from Kafka or connecting to database: {e}")
finally:
    cursor.close()
    conn.close()
    logger.info("Database connection closed")
