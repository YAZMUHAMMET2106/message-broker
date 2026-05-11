# Message Broker — Delivery Guarantees Module
## Брокер сообщений — Модуль Гарантий Доставки

Проект: брокер сообщений в экосистеме Linux  
Язык: Python 3.10+  
БД: SQLite (встроенная, без дополнительных зависимостей)

---

## Архитектура

```
message_broker/
├── main.py               ← точка входа (запуск сервера)
├── demo.py               ← демонстрация всех режимов доставки
├── requirements.txt
├── broker/
│   ├── server.py         ← TCP-сервер брокера
│   └── delivery.py       ← модуль гарантий доставки ★
├── client/
│   └── client.py         ← Producer и Consumer классы
├── database/
│   └── db.py             ← слой персистентности (SQLite)
├── tests/
│   └── test_delivery.py  ← юнит-тесты
└── logs/
    └── broker.log
```

---

## Режимы доставки (Delivery Modes)

| Режим | Описание | ACK/NACK | Повторы |
|-------|----------|----------|---------|
| `at_most_once` | Отправить и забыть | Нет | Нет |
| `at_least_once` | Повторять до подтверждения | Да | Да (до max_retries) |
| `exactly_once` | Ровно один раз (дедупликация) | Да | Да + dedup key |

---

## Запуск

### 1. Запуск брокера
```bash
cd message_broker
python main.py
# или с параметрами:
python main.py --host 0.0.0.0 --port 9999 --db broker.db --log DEBUG
```

### 2. Демонстрация
```bash
python demo.py
```

### 3. Тесты
```bash
python -m pytest tests/ -v
# или без pytest:
python tests/test_delivery.py
```

---

## Протокол (JSON over TCP)

### Публикация сообщения
```json
{"cmd": "publish", "topic": "orders", "payload": "{...}", "mode": "at_least_once", "producer_id": "p1"}
← {"status": "ok", "message_id": 42}
```

### Подписка
```json
{"cmd": "subscribe", "topic": "orders", "consumer_id": "worker-1"}
← {"status": "ok", "consumer_id": "worker-1", "subscribed_to": "orders"}
```

### Получение сообщения (от брокера)
```json
← {"type": "message", "message_id": 42, "topic": "orders", "payload": "{...}", "mode": "at_least_once"}
```

### ACK / NACK
```json
{"cmd": "ack",  "message_id": 42, "consumer_id": "worker-1"}
{"cmd": "nack", "message_id": 42, "consumer_id": "worker-1", "error": "parse error"}
```

### Статистика
```json
{"cmd": "stats"}
← {"status": "ok", "stats": {"pending": 5, "acknowledged": 100, ...}}
```

---

## Пример использования в коде

```python
from client.client import Producer, Consumer

# Производитель
p = Producer(host="127.0.0.1", port=9999)
p.connect()
msg_id = p.publish("payments", '{"amount": 100}', mode="exactly_once")
p.disconnect()

# Потребитель
def handle_payment(msg_id, topic, payload):
    print(f"Processing payment: {payload}")
    return True  # True = ACK, False = NACK

c = Consumer("payment-worker", host="127.0.0.1", port=9999)
c.connect()
c.subscribe("payments", handle_payment)
c.listen()  # блокирующий вызов
```
