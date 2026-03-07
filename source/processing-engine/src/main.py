import pika
import json
import os
import time
import threading # Fondamentale per fare due cose insieme
from flask import Flask, request, jsonify # Per parlare col frontend

import database
from entities import State, Rule # Importiamo entrambi

app = Flask(__name__)

def get_connection():
    rabbit_host = os.getenv('RABBITMQ_HOST', 'localhost')
    while True:
        try:
            print("Testing connection to RabbitMQ...")
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=rabbit_host)
            )
            print("Successful connection")
            return connection
        except:
            print(" [!] RabbitMQ not yet started. Retry in 5 seconds.")
            time.sleep(5)

def inject_callback(state):
    def callback(ch, method, properties, body):
        data = json.loads(body)
        print(f"Ricevuto dati : {data}")
        state.update(data)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    return callback

def start_consuming(state):
    connection = get_connection()
    channel = connection.channel()
    channel.exchange_declare(exchange='mars_telemetry_exchange', exchange_type='fanout')
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange='mars_telemetry_exchange', queue=queue_name)

    channel.basic_consume(
        queue=queue_name, 
        on_message_callback=inject_callback(state), 
        auto_ack=False
    )
    
    print("[*] Processing Engine in ascolto su RabbitMQ...")
    channel.start_consuming()

# --- NUOVE ROTTE PER IL FRONTEND ---

@app.route('/rules', methods=['GET', 'POST'])
def handle_rules():
    if request.method == 'POST':
        data = request.json
        # Trasformiamo il JSON in un oggetto Rule
        row = [
            data['sensor_name'], 
            data['metric'], 
            data['operator'], 
            float(data['sensor_target_value']), 
            data['actuator_name'], 
            data['actuator_set_value'], 
            True # enabled
        ]
        new_rule = Rule(row)
        state.create_new_rule(new_rule)
        return jsonify({"status": "success"}), 201
    
    else:
        # GET: Mandiamo al frontend la lista di tutte le regole
        all_rules = []
        for sensor in state.current_rules:
            for r in state.current_rules[sensor]:
                all_rules.append({
                    "sensor_name": r.sensor_name,
                    "metric": r.metric,
                    "operator": r.operator,
                    "sensor_target_value": r.sensor_target_value,
                    "actuator_name": r.actuator_name,
                    "actuator_set_value": r.actuator_set_value
                })
        return jsonify(all_rules)

if __name__ == "__main__":
    database.init_db()
    
    # Rendiamo 'state' accessibile a Flask
    global state
    state = State()
    state.load_persistent_rules()
    state.load_persistent_actuators()
    
    # AVVIAMO RABBITMQ IN UN THREAD SEPARATO
    # Se non facciamo così, Flask non partirebbe mai!
    rabbit_thread = threading.Thread(target=start_consuming, args=(state,), daemon=True)
    rabbit_thread.start()
    
    # AVVIAMO FLASK (Porta 8001 come deciso prima)
    print("[*] Processing Engine API in avvio sulla porta 8001...")
    app.run(host="0.0.0.0", port=8001)