import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

pages = ['/home', '/about', '/products', '/contact']
users = [f'user_{i}' for i in range(1, 6)]
event_types = ['page_view', 'page_view', 'page_view', 'click']

while True:
    event = {
        "event_time": (datetime.utcnow() - timedelta(seconds=random.randint(0, 120))).isoformat(),
        "user_id": random.choice(users),
        "page_url": random.choice(pages),
        "event_type": random.choice(event_types)
    }
    producer.send('user_activity', value=event)
    print(event)
    time.sleep(1)