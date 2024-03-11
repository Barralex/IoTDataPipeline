import threading
import json
from kafka import KafkaProducer
import time
import logging
import random
import argparse
import signal
from tabulate import tabulate


keep_running = True


def signal_handler(signum, frame):
    global keep_running
    keep_running = False
    print("Interruption detected, stopping producers... 🛑")


signal.signal(signal.SIGINT, signal_handler)


class ProducerThread(threading.Thread):
    def __init__(self, run_time_minutes, device_key, topic, broker_address):
        threading.Thread.__init__(self)
        self.run_time_minutes = run_time_minutes
        self.device_key = device_key
        self.topic = topic
        self.broker_address = broker_address
        self.running = True
        self.message_count = 0

    def stop(self):
        self.running = False

    def run(self):
        producer = KafkaProducer(bootstrap_servers=[self.broker_address], max_block_ms=5000)
        end_time = time.time() + self.run_time_minutes * 60

        while self.running and time.time() < end_time and keep_running:
            try:
                temperature = random.uniform(20.0, 30.0)

                res = {'timestamp': time.time(), 'temperature': temperature, 'device_key': self.device_key}
                producer.send(self.topic, json.dumps(res).encode('utf-8'))
                self.message_count += 1
                print(f"{self.device_key} -> {json.dumps(res)}")
            except Exception as e:
                logging.error(f'An error occurred: {e}')
                continue

        producer.flush()
        producer.close()

    def get_message_count(self):
        return self.message_count


def main(producers, duration):
    threads = []
    for i in range(producers):
        device_key = f"dev-{random.randint(1000, 9999)}"
        thread = ProducerThread(duration, device_key, 'iot_temperature_readings', 'localhost:9092')
        thread.start()
        threads.append(thread)

    try:
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        print("Stopping all threads... 🛑")
        for thread in threads:
            thread.stop()

    results = [[thread.device_key, thread.get_message_count()] for thread in threads]
    total_messages = sum([count for _, count in results])
    results.append(["Total", f"{total_messages:,}"])
    print(tabulate(results, headers=['Device Key', 'Messages Sent'], tablefmt='grid'))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run Kafka producer threads.')
    parser.add_argument('-producers', type=int, help='Number of producer threads to run', required=True)
    parser.add_argument('-duration', type=int, help='Duration for which each producer should run (in minutes)', required=True)

    args = parser.parse_args()
    main(args.producers, args.duration)

