import json
import time
import random
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer
from clickhouse_driver import Client
from clickhouse_driver.errors import Error
import logging
import os
import socket

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('data_generator')

fake = Faker('ru_RU')


class ClickHouseInitializer:
    def __init__(self, host='clickhouse', port=9000, user='default', password=''):
        self.client = Client(
            host=host,
            port=port,
            user=user,
            password=password,
            connect_timeout=10,
            send_receive_timeout=30
        )

    def wait_for_clickhouse(self, max_retries=30, retry_delay=5):
        """Ожидание доступности ClickHouse"""
        logger.info("Ожидаем доступность ClickHouse...")

        for attempt in range(max_retries):
            try:
                self.client.execute('SELECT 1')
                logger.info("ClickHouse доступен")
                return True
            except Error as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Попытка {attempt + 1}/{max_retries}: ClickHouse еще не доступен")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"ClickHouse не стал доступен после {max_retries} попыток: {e}")
                    return False
        return False

    def wait_for_kafka_connection(self, max_retries=30, retry_delay=5):
        """Ожидание, пока Kafka станет доступна для ClickHouse"""
        logger.info("Проверяем подключение ClickHouse к Kafka...")

        for attempt in range(max_retries):
            try:
                # Пробуем создать временную Kafka таблицу для проверки подключения
                self.client.execute('''
                CREATE TABLE IF NOT EXISTS test_kafka_connection (
                    dummy String
                ) ENGINE = Kafka()
                SETTINGS
                    kafka_broker_list = 'kafka:29092',
                    kafka_topic_list = 'test_topic',
                    kafka_group_name = 'test_connection',
                    kafka_format = 'JSONEachRow',
                    kafka_skip_broken_messages = 1
                ''')

                # Если не было исключения - подключение успешно
                self.client.execute('DROP TABLE IF EXISTS test_kafka_connection')
                logger.info("Kafka доступна для ClickHouse")
                return True

            except Error as e:
                if "Cannot assign requested address" in str(e) or "Connection refused" in str(e):
                    if attempt < max_retries - 1:
                        logger.warning(f"Попытка {attempt + 1}/{max_retries}: Kafka еще не доступна для ClickHouse")
                        time.sleep(retry_delay)
                    else:
                        logger.error(f"Kafka не стала доступна для ClickHouse после {max_retries} попыток")
                        return False
                else:
                    # Другие ошибки - возможно, Kafka доступна, но есть другие проблемы
                    logger.warning(f"Ошибка подключения к Kafka: {e}")
                    return True

        return False

    def create_kafka_tables(self):
        """Создание таблиц с движком Kafka"""
        logger.info("Создаем Kafka таблицы...")

        try:
            # Таблица для sales
            self.client.execute('''
            CREATE TABLE IF NOT EXISTS sales_kafka (
                event_id String,
                event_type String,
                event_time String,
                product_id UInt64,
                product_name String,
                category String,
                quantity UInt32,
                price Float64,
                discount Float64,
                total Float64,
                store_id UInt32,
                cashier_id UInt32,
                customer_id String
            ) ENGINE = Kafka()
            SETTINGS
                kafka_broker_list = 'kafka:29092',
                kafka_topic_list = 'sales',
                kafka_group_name = 'clickhouse_sales_consumer',
                kafka_format = 'JSONEachRow'
            ''')

            # Таблица для warehouse
            self.client.execute('''
            CREATE TABLE IF NOT EXISTS warehouse_kafka (
                event_id String,
                event_type String,
                event_time String,
                product_id UInt64,
                product_name String,
                category String,
                warehouse String,
                quantity Int32,
                movement_type String,
                source String,
                responsible String
            ) ENGINE = Kafka()
            SETTINGS
                kafka_broker_list = 'kafka:29092',
                kafka_topic_list = 'warehouse',
                kafka_group_name = 'clickhouse_warehouse_consumer',
                kafka_format = 'JSONEachRow'
            ''')

            logger.info("Kafka таблицы созданы")
            return True

        except Error as e:
            logger.error(f"Ошибка при создании Kafka таблиц: {e}")
            return False

    def create_target_tables(self):
        """Создание целевых таблиц"""
        logger.info("Создаем целевые таблицы...")

        try:
            # Таблица для продаж
            self.client.execute('''
            CREATE TABLE IF NOT EXISTS sales (
                event_id String,
                event_type String,
                event_time DateTime,
                product_id UInt64,
                product_name String,
                category String,
                quantity UInt32,
                price Decimal32(2),
                discount Decimal32(2),
                total Decimal32(2),
                store_id UInt32,
                cashier_id UInt32,
                customer_id String,
                processing_time DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMM(event_time)
            ORDER BY (event_time, product_id)
            SETTINGS index_granularity = 8192
            ''')

            # Таблица для движений склада
            self.client.execute('''
            CREATE TABLE IF NOT EXISTS stock_movements (
                event_id String,
                event_type String,
                event_time DateTime,
                product_id UInt64,
                product_name String,
                category String,
                warehouse String,
                quantity Int32,
                movement_type String,
                source String,
                responsible String,
                processing_time DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMM(event_time)
            ORDER BY (event_time, product_id)
            SETTINGS index_granularity = 8192
            ''')

            logger.info("Целевые таблицы созданы")
            return True

        except Error as e:
            logger.error(f"Ошибка при создании целевых таблиц: {e}")
            return False

    def create_materialized_views(self):
        """Создание материализованных представлений без фильтрации по времени"""
        logger.info("Создаем материализованные представления...")

        try:
            # MV для продаж - без фильтрации по event_time
            self.client.execute('''
            CREATE MATERIALIZED VIEW IF NOT EXISTS sales_mv TO sales AS
            SELECT 
                event_id,
                event_type,
                parseDateTimeBestEffortOrNull(event_time) as event_time,
                product_id,
                product_name,
                category,
                quantity,
                toDecimal32(price, 2) as price,
                toDecimal32(discount, 2) as discount,
                toDecimal32(total, 2) as total,
                store_id,
                cashier_id,
                customer_id
            FROM sales_kafka
            WHERE price IS NOT NULL
            ''')

            # MV для движений склада - без фильтрации по event_time
            self.client.execute('''
            CREATE MATERIALIZED VIEW IF NOT EXISTS stock_movements_mv TO stock_movements AS
            SELECT 
                event_id,
                event_type,
                parseDateTimeBestEffortOrNull(event_time) as event_time,
                product_id,
                product_name,
                category,
                warehouse,
                quantity,
                movement_type,
                source,
                responsible
            FROM warehouse_kafka
            ''')

            logger.info("Материализованные представления созданы")
            return True

        except Error as e:
            logger.error(f"Ошибка при создании MV: {e}")
            return False

    def initialize_all_tables(self):
        """Полная инициализация всех таблиц после доступности Kafka"""
        if not self.wait_for_clickhouse():
            return False

        # Ждем, пока Kafka станет доступна для ClickHouse
        if not self.wait_for_kafka_connection():
            logger.warning("Kafka не доступна, создаем только целевые таблицы")
            return self.create_target_tables()

        try:
            # Создаем все таблицы
            success = True
            success &= self.create_target_tables()
            success &= self.create_kafka_tables()
            success &= self.create_materialized_views()

            if success:
                # ОТКЛЮЧАЕМ потребление сообщений сразу после создания
                self.detach_kafka_tables()

                # Проверяем создание таблиц
                tables = self.client.execute('SHOW TABLES')
                logger.info(f"Доступные таблицы: {[table[0] for table in tables]}")
                logger.info("Все таблицы успешно созданы и потребление отключено!")
                logger.info("Для запуска потребления выполните:")
                logger.info("ATTACH TABLE sales_kafka;")
                logger.info("ATTACH TABLE warehouse_kafka;")

            return success

        except Error as e:
            logger.error(f"Ошибка при создании таблиц: {e}")
            return False

    def detach_kafka_tables(self):
        """Отключение потребления из Kafka таблиц"""
        logger.info("Отключаем потребление из Kafka таблиц...")

        try:
            self.client.execute('DETACH TABLE IF EXISTS sales_kafka')
            self.client.execute('DETACH TABLE IF EXISTS warehouse_kafka')
            logger.info("Потребление из Kafka отключено")
            return True
        except Error as e:
            logger.error(f"Ошибка при отключении Kafka таблиц: {e}")
            return False


def wait_for_kafka_producer(kafka_host, kafka_port, max_retries=30, retry_delay=5):
    """Ожидание доступности Kafka для продюсера"""
    logger.info(f"Ожидаем доступность Kafka для продюсера на {kafka_host}:{kafka_port}...")

    for attempt in range(max_retries):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((kafka_host, kafka_port))
            sock.close()

            if result == 0:
                logger.info(f"Kafka доступен для продюсера на {kafka_host}:{kafka_port}")
                return True
            else:
                logger.warning(f"Попытка {attempt + 1}/{max_retries}: Kafka еще не доступен для продюсера")

        except Exception as e:
            logger.warning(f"Попытка {attempt + 1}/{max_retries}: Ошибка подключения - {e}")

        time.sleep(retry_delay)

    logger.error(f"Kafka не стал доступен для продюсера после {max_retries} попыток")
    return False


class DataGenerator:
    def __init__(self, kafka_broker):
        self.kafka_broker = kafka_broker
        self.producer = None
        self.products = self._generate_products(50)
        self.warehouses = ['Москва', 'Волгоград', 'Санкт-Петербург', 'Казань', 'Новосибирск', 'Екатеринбург']

    def initialize_clickhouse_tables(self):
        """Инициализация таблиц в ClickHouse после доступности Kafka"""
        logger.info("Инициализируем таблицы в ClickHouse...")

        try:
            initializer = ClickHouseInitializer()
            return initializer.initialize_all_tables()
        except Exception as e:
            logger.error(f"Не удалось инициализировать таблицы: {e}")
            return False

    def initialize_producer(self):
        """Инициализация продюсера"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[self.kafka_broker],
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                acks=1,
                retries=3,
                request_timeout_ms=10000,
                api_version=(2, 8, 0)
            )
            logger.info(f"Продюсер инициализирован для {self.kafka_broker}")
            return True
        except Exception as e:
            logger.error(f"Ошибка инициализации продюсера: {e}")
            return False

    def _generate_products(self, count):
        return [{
            'id': i + 1,
            'name': f"{fake.word()} {fake.word()}",
            'category': random.choice(['Электроника', 'Одежда', 'Продукты', 'Книги', 'Бытовая техника']),
            'price': round(random.uniform(100, 10000), 2),
            'base_cost': round(random.uniform(50, 5000), 2)
        } for i in range(count)]

    def generate_sale(self):
        product = random.choice(self.products)
        sale = {
            'event_id': fake.uuid4(),
            'event_type': 'sale',
            'event_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'product_id': product['id'],
            'product_name': product['name'],
            'category': product['category'],
            'quantity': random.randint(1, 5),
            'price': float(product['price']),
            'discount': float(round(random.uniform(0, 0.3), 2)),
            'total': float(round(product['price'] * (1 - random.uniform(0, 0.3)), 2)),
            'store_id': random.randint(1, 10),
            'cashier_id': random.randint(1, 20),
            'customer_id': fake.uuid4(),
        }
        return sale

    def generate_stock_movement(self):
        product = random.choice(self.products)
        movement = {
            'event_id': fake.uuid4(),
            'event_type': 'stock_movement',
            'event_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'product_id': product['id'],
            'product_name': product['name'],
            'category': product['category'],
            'warehouse': random.choice(self.warehouses),
            'quantity': random.randint(1, 100),
            'movement_type': random.choice(['supply', 'relocation', 'write_off']),
            'source': fake.company(),
            'responsible': fake.name()
        }
        return movement

    def run(self):
        logger.info("Генератор данных запущен")

        # Парсим хост и порт из строки подключения
        kafka_host, kafka_port_str = self.kafka_broker.split(':')
        kafka_port = int(kafka_port_str)

        # Ожидаем доступность Kafka для продюсера
        if not wait_for_kafka_producer(kafka_host, kafka_port):
            logger.error("Не удалось подключиться к Kafka, завершаем работу")
            return

        # Инициализируем таблицы в ClickHouse (после доступности Kafka)
        if not self.initialize_clickhouse_tables():
            logger.warning("Не удалось создать все таблицы, но продолжаем работу")

        # Инициализируем продюсер
        if not self.initialize_producer():
            logger.error("Не удалось инициализировать продюсер")
            return

        logger.info("Начинаем генерацию данных...")

        message_count = 0
        while True:
            try:
                if random.random() < 0.7:
                    event = self.generate_sale()
                    topic = 'sales'
                else:
                    event = self.generate_stock_movement()
                    topic = 'warehouse'

                self.producer.send(topic, event)
                message_count += 1

                if message_count % 10 == 0:
                    logger.info(f"Отправлено {message_count} сообщений в Kafka")

                time.sleep(random.uniform(0.5, 1.5))

            except Exception as e:
                logger.error(f"Ошибка при отправке: {e}")
                time.sleep(5)


if __name__ == "__main__":
    kafka_broker = os.getenv('KAFKA_BROKER', 'kafka:29092')
    logger.info(f"Запуск генератора данных для Kafka: {kafka_broker}")

    generator = DataGenerator(kafka_broker)
    generator.run()
