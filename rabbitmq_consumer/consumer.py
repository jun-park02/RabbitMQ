from dotenv import load_dotenv
import pika
import os
import sys

load_dotenv()

def callback(ch, method, properties, body):
    message = body.decode()

    print(f"[RabbitMQ Container Received] : {message}")

    # 여기에 로직 넣고
    pass

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