import pika
import json
import os
import time

import database

def get_connection():
    rabbit_host = os.getenv('RABBITMQ_HOST', 'localhost')
    while True:
        try:
            print("Testing connection")
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=rabbit_host)
            )
            print("Successful connection")
            return connection
        except:
            print(" [!] RabbitMQ not yeat started. Retry in 2 seconds.")
            time.sleep(2)


def callback(ch, method, properties, body):
    data = json.loads(body)
    print(f"Ricevuto dati : {data}\n\n")
    
    # TODO: logica delle regole    

    ch.basic_ack(delivery_tag=method.delivery_tag)


def start_consuming():
    connection = get_connection()
    channel = connection.channel()

    channel.queue_declare(queue='sensor_data', durable=False)
    
    channel.basic_consume(
        queue='sensor_data', 
        on_message_callback=callback, 
        auto_ack=False
    )
    channel.start_consuming()

if __name__ == "__main__":
    database.init_db()
    start_consuming()