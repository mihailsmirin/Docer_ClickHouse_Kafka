# Демонстрация интеграции Kafka + ClickHouse

[![Docker](https://img.shields.io/badge/Docker-✓-blue)](https://docker.com)
[![Kafka](https://img.shields.io/badge/Kafka-✓-blue)](https://kafka.apache.org)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-✓-blue)](https://clickhouse.com)
[![Real-time](https://img.shields.io/badge/Real--time-✓-green)](https://clickhouse.com)

Данная дипломная работа демонстрирует современный подход к построению потоковых ETL-конвейеров с использованием **Apache Kafka** в
качестве брокера сообщений и **ClickHouse** как высокопроизводительной колоночной СУБД для аналитики.

## 🏗️ Архитектура решения

### Основной поток данных

```text
[Data Generator] → [Kafka Topics] → [Kafka Engine Tables] → [Materialized Views] → [Target Tables] → [Web Dashboard]
```

### Детальная схема

```text
Python App → Kafka (sales topic) → sales_kafka (Kafka Engine) → sales_mv (MV) → sales (MergeTree) → Dashboard
Python App → Kafka (warehouse topic) → warehouse_kafka (Kafka Engine) → stock_movements_mv (MV) → stock_movements (MergeTree) → Dashboard
```

## 📊 Структура данных

### Топики Kafka

- **`sales`** - данные о продажах (70% трафика)
- **`warehouse`** - движения товаров на складе (30% трафика)

### Таблицы ClickHouse

| Таблица                  | Тип               | Назначение                    |
|--------------------------|-------------------|-------------------------------|
| **`sales_kafka`**        | Kafka Engine      | Приемник для топика sales     |
| **`warehouse_kafka`**    | Kafka Engine      | Приемник для топика warehouse |
| **`sales_mv`**           | Materialized View | Трансформация данных продаж   |
| **`stock_movements_mv`** | Materialized View | Трансформация движений склада |
| **`sales`**              | MergeTree         | Финальные данные продаж       |
| **`stock_movements`**    | MergeTree         | Движения товаров              |

## 🚀 Быстрый старт (3 шага)

### Предварительные требования

- **Docker** & **Docker Compose**
- 4+ Гб ОЗУ
- 2+ ядра CPU

### 1. Запуск инфраструктуры

```bash
# Клонирование и запуск
git clone https://github.com/mihailsmirin/Docer_ClickHouse_Kafka
cd Docer_ClickHouse_Kafka
docker-compose up -d
```

### 2. Активация потребления данных

```bash
# Подключение к ClickHouse
docker-compose exec clickhouse clickhouse-client

# Активация приема данных из Kafka
ATTACH TABLE sales_kafka;
ATTACH TABLE warehouse_kafka;
```

### 3. Открытие дашборда

```
Откройте в браузере: http://localhost:3000
```

## ⚡ Механика работы

### 1. Генерация тестовых данных

```python
# Автоматическая генерация 1-2 сообщения/секунду
producer.send('sales', {
    'event_id': 'uuid',
    'event_time': '2024-01-15 10:00:00',
    'product_id': 123,
    'price': 100.50,
    'quantity': 2
})
```

### 2. Real-time потребление через Kafka Engine

```sql
-- Автоматическое потребление из топиков
CREATE TABLE sales_kafka (...) ENGINE = Kafka(
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'sales',
    kafka_format = 'JSONEachRow'
);
```

### 3. Автоматическая трансформация

```sql
-- Materialized Views преобразуют данные на лету
CREATE MATERIALIZED VIEW sales_mv TO sales AS
SELECT 
    parseDateTimeBestEffort(event_time) as event_time,
    toDecimal32(price, 2) as price,
    -- ... трансформация полей
FROM sales_kafka;
```

### 4. Визуализация в реальном времени

```javascript
// Дашборд обновляется каждые 5 секунд
setInterval(loadData, 5000); // Real-time обновления
```

## 🎯 Ключевые особенности

### ✅ Реализовано в проекте

- **Полный ETL-конвейер** в Docker-окружении
- **Real-time потребление** из Kafka топиков (1-2 сек задержки)
- **Автоматическая трансформация** через Materialized Views
- **Готовый дашборд** с визуализацией в реальном времени

## 🎥 Видео-демонстрация

Проект включает демо-видео, показывающее:

- ✅ Запуск Docker-контейнеров 
- ✅ Активацию приема данных из Kafka в реальном времени
- ✅ Работу дашборда с обновлением в реальном времени
- ✅ Генерацию тестовых данных и их отображение

[**Смотреть демо-видео**](https://disk.yandex.ru/i/gm4MXOuTgg_GOA)

**Технологический стек:** Docker · Kafka · ClickHouse · Python · Flask · Chart.js

**Статус проекта:** ✅ Рабочий прототип · 🚀 Готов к демонстрации · 📊 Real-time analytics
