from flask import Flask, render_template
import psycopg2
from config import db_config

app = Flask(__name__)


def get_stock_data():
    conn = psycopg2.connect(
        host=db_config.DB_HOST,
        port=db_config.DB_PORT,
        dbname=db_config.DB_NAME,
        user=db_config.DB_USER,
        password=db_config.DB_PASSWORD
    )
    cursor = conn.cursor()
    cursor.execute(
        "SELECT symbol, price, timestamp FROM stocks ORDER BY timestamp DESC LIMIT 10")
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data


@app.route('/')
def index():
    stock_data = get_stock_data()
    return render_template('index.html', stock_data=stock_data)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
