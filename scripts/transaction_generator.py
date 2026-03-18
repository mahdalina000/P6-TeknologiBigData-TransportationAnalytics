import json
import random
import time
import os
from datetime import datetime

os.makedirs("stream_data", exist_ok=True)

products = ["Laptop", "Mouse", "Keyboard", "Monitor", "Headset", "Webcam"]
cities = ["Jakarta", "Bandung", "Surabaya", "Medan", "Yogyakarta"]

print("🔥 Generator Running...")
counter = 1
while True:
    transaction = {
        "user_id": random.randint(100, 200),
        "product": random.choice(products),
        "price": random.randint(50, 2000),
        "city": random.choice(cities),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    with open(f"stream_data/transaction_{counter}.json", "w") as f:
        json.dump(transaction, f)
    print(f"✅ Sent: {transaction['product']} to {transaction['city']}")
    counter += 1
    time.sleep(3)