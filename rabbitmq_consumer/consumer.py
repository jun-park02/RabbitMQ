import pika
import os
import sys
import requests
import json

def send_pushover_notification(message):
    token = os.getenv("PUSHOVER_TOKEN")
    user = os.getenv("PUSHOVER_USER")
    url = "https://api.pushover.net/1/messages.json"
    data = {
        "token": token,
        "user": user,
        "message": message
    }

    requests.post(url, data)


def callback(ch, method, properties, body):
    event = json.loads(body.decode("utf-8"))
    message = event["content"]

    print(f"[RabbitMQ Container Received] : {message}")

    send_pushover_notification(message)

    # 처리 완료 ACK
    ch.basic_ack(delivery_tag=method.delivery_tag)


class RabbitMQ:
    host = "rabbitmq"
    queue_name = "message_queue"

    def __init__(self):
        user = os.getenv("RABBITMQ_DEFAULT_USER")
        password = os.getenv("RABBITMQ_DEFAULT_PASS")
        credentials = pika.PlainCredentials(user, password)

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.host,
                port=5672,
                credentials=credentials
            )
        )

        self.channel = self.connection.channel()

        self.channel.queue_declare(
            queue=self.queue_name,
            durable=True
        )

        # 한 번에 하나씩 처리
        self.channel.basic_qos(prefetch_count=1)

    def start(self):
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=callback
        )

        self.channel.start_consuming()

if __name__ == "__main__":
    try: 
        rabbitmq = RabbitMQ()
        rabbitmq.start()
    except Exception as e:
        print(e)
        sys.exit(1)