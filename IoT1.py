import random
import time
import json
from datetime import datetime

def emulate_iot_device():
    while True:
        temperature_data = {
            'device_id': 'device_001',
            'temperature': round(random.uniform(20.0, 30.0), 2),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        send_data(temperature_data)
        time.sleep(1)

def send_data(data):
    print(json.dumps(data))

def main():
    emulate_iot_device()

if __name__ == "__main__":
    main()

