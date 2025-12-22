from flask import Flask, render_template, jsonify
from clickhouse_driver import Client
import time
import logging

app = Flask(__name__)

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('web-app')

# Подключение к ClickHouse
ch_client = Client(host='clickhouse')


def check_tables_exist():
    """Проверка существования необходимых таблиц"""
    try:
        tables = ch_client.execute("SHOW TABLES")
        existing_tables = [table[0] for table in tables]

        required_tables = ['sales', 'stock_movements']
        missing_tables = [table for table in required_tables if table not in existing_tables]

        if missing_tables:
            logger.warning(f"Отсутствуют таблицы: {missing_tables}")
            return False

        return True

    except Exception as e:
        logger.error(f"Ошибка при проверке таблиц: {e}")
        return False


def get_fallback_data():
    """Возвращает заглушки для данных пока таблицы не созданы"""
    return {
        'labels': [],
        'quantity': [],
        'revenue': []
    }


@app.route('/')
def dashboard():
    return render_template('index.html')


@app.route('/api/sales')
def get_sales():
    """Получение данных о продажах за последние 24 часа"""
    if not check_tables_exist():
        return jsonify(get_fallback_data())

    try:
        query = """
        SELECT 
            toStartOfHour(event_time) as hour,
            sum(quantity) as total_quantity,
            sum(total) as revenue
        FROM sales
        WHERE event_time >= now() - INTERVAL 1 DAY
        GROUP BY hour
        ORDER BY hour
        """
        data = ch_client.execute(query)

        return jsonify({
            'labels': [row[0].strftime('%H:%M') for row in data],
            'quantity': [row[1] for row in data],
            'revenue': [round(float(row[2]), 2) for row in data]
        })

    except Exception as e:
        logger.error(f"Ошибка при получении данных продаж: {e}")
        return jsonify(get_fallback_data())


@app.route('/api/stock')
def get_stock_movements():
    """Топ 5 товаров по движениям на складе"""
    if not check_tables_exist():
        return jsonify({
            'products': [],
            'incoming': [],
            'outgoing': []
        })

    try:
        query = """
        SELECT 
            product_id,
            sum(if(movement_type='supply', quantity, 0)) as incoming,
            sum(if(movement_type IN ('relocation', 'write_off'), quantity, 0)) as outgoing
        FROM stock_movements
        WHERE event_time >= now() - INTERVAL 7 DAY
        GROUP BY product_id
        ORDER BY (incoming + outgoing) DESC
        LIMIT 5
        """
        data = ch_client.execute(query)

        return jsonify({
            'products': [f"Товар {row[0]}" for row in data],
            'incoming': [row[1] for row in data],
            'outgoing': [row[2] for row in data]
        })

    except Exception as e:
        logger.error(f"Ошибка при получении данных склада: {e}")
        return jsonify({
            'products': [],
            'incoming': [],
            'outgoing': []
        })


@app.route('/api/recent_sales')
def get_recent_sales():
    """Последние 10 продаж"""
    if not check_tables_exist():
        return jsonify([])

    try:
        query = """
        SELECT 
            product_id,
            quantity,
            price,
            event_time
        FROM sales 
        ORDER BY event_time DESC 
        LIMIT 10
        """
        data = ch_client.execute(query)

        return jsonify([{
            'product_id': row[0],
            'quantity': row[1],
            'price': float(row[2]),
            'event_time': row[3].strftime('%Y-%m-%d %H:%M:%S')
        } for row in data])

    except Exception as e:
        logger.error(f"Ошибка при получении последних продаж: {e}")
        return jsonify([])


@app.route('/api/status')
def get_status():
    """Статус системы и таблиц"""
    try:
        tables = ch_client.execute("SHOW TABLES")
        existing_tables = [table[0] for table in tables]

        # Проверяем наличие данных в таблицах
        sales_count = ch_client.execute("SELECT count() FROM sales")[0][0] if 'sales' in existing_tables else 0
        stock_count = ch_client.execute("SELECT count() FROM stock_movements")[0][
            0] if 'stock_movements' in existing_tables else 0

        return jsonify({
            'tables_exist': 'sales' in existing_tables and 'stock_movements' in existing_tables,
            'existing_tables': existing_tables,
            'sales_count': sales_count,
            'stock_count': stock_count,
            'status': 'ready' if sales_count > 0 else 'waiting_for_data'
        })

    except Exception as e:
        logger.error(f"Ошибка при проверке статуса: {e}")
        return jsonify({
            'tables_exist': False,
            'existing_tables': [],
            'sales_count': 0,
            'stock_count': 0,
            'status': 'error'
        })


if __name__ == '__main__':
    logger.info("Запуск web-приложения...")
    app.run(host='0.0.0.0', port=3000)