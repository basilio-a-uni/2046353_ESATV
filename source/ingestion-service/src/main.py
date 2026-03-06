# import messaging
# import websocket
# import json
# import threading
# import time
# import requests


# def on_message(ws, message):
#     """Questa funzione scatta automaticamente quando arriva un dato dal WebSocket"""
#     raw_data = json.loads(message)
#     print(f"[STREAM] Nuovo dato ricevuto: {raw_data}")
    
#     # -> QUI trasformerai raw_data nel tuo Unified Schema
#     # -> QUI invierai il dato a RabbitMQ

# def on_error(ws, error):
#     print(f"Errore WebSocket: {error}")

# def on_close(ws, close_status_code, close_msg):
#     print("Connessione WebSocket chiusa.")

# def start_ws_stream(topic):
#     """Apre una connessione WebSocket per un topic specifico"""
#     # L'URL esatto fornito dalle tue specifiche
#     ws_url = f"ws://simulator:8080/api/telemetry/ws?topic={topic}"
    
#     ws = websocket.WebSocketApp(
#         ws_url,
#         on_message=on_message,
#         on_error=on_error,
#         on_close=on_close
#     )
#     # Avvia l'ascolto infinito
#     ws.run_forever()




# if __name__ == "__main__":
#     data_example = {
#         "timestamp": "2026-03-06T11:45:00",
#         "rest_sensors": {"greenhouse_temp": 22.5, "co2_hall": 450},
#         "telemetry": {"topic_alpha": 10.2}
#     }

#     messaging.send_message(data_example)


# if __name__ == "__main__":
#     print("Inizializzazione Ingestion Service su Marte...")
    
#     # 1. Avvia il thread per il polling dei sensori REST
#     rest_thread = threading.Thread(target=poll_rest_sensors)
#     rest_thread.daemon = True
#     rest_thread.start()
    
#     # 2. Scopri i topic di telemetria e avvia un WebSocket per ciascuno
#     try:
#         # Usa l'endpoint indicato nelle tue docs per scoprire i topic
#         topics_response = requests.get("http://simulator:8080/api/telemetry/topics")
#         topics_list = topics_response.json()
        
#         for topic in topics_list:
#             # Crea un thread separato per ascoltare ogni singolo topic
#             ws_thread = threading.Thread(target=start_ws_stream, args=(topic,))
#             ws_thread.daemon = True
#             ws_thread.start()
#             print(f"Avviato ascolto stream per {topic}")
            
#     except Exception as e:
#         print(f"Impossibile recuperare i topic di telemetria: {e}")

#     # Mantieni il container attivo
#     while True:
#         time.sleep(1)





# SSECLIENT ---------------------------


# import requests
# import json
# from sseclient import SSEClient


# def test_stream():
#     # IL TRUCCO È QUI: usiamo host.docker.internal invece di localhost
#     url = "http://host.docker.internal:8080/api/telemetry/stream/mars/telemetry/solar_array"
    
#     print(f"📡 Tentativo di connessione a {url}...")
    
#     try:
#         # stream=True mantiene la connessione aperta
#         response = requests.get(url, stream=True)
#         response.raise_for_status() # Lancia un errore se lo status non è 200 OK
        
#         client = SSEClient(response)
#         print("✅ Connessione stabilita! In attesa della telemetria...")
        
#         # Ciclo infinito che stampa i dati man mano che arrivano
#         for event in client.events():
#             if event.data:
#                 data = json.loads(event.data)
#                 print(f"🪐 Dati da Marte: {data}")
                
#     except Exception as e:
#         print(f"❌ Errore: {e}")
#         print("💡 Se 'host.docker.internal' fallisce, metti al suo posto l'IP LAN del tuo PC (es. 192.168.1.X).")

# if __name__ == "__main__":
#     test_stream()








import messaging
import asyncio
import websockets
import aiohttp
import json

TOPICS = [
    T_SOLAR_ARRAY := "mars/telemetry/solar_array",
    T_RADIATION := "mars/telemetry/radiation",
    T_LIFE_SUPPORT := "mars/telemetry/life_support",
    T_THERMAL_LOOP := "mars/telemetry/thermal_loop",
    T_POWER_BUS := "mars/telemetry/power_bus",
    T_POWER_CONSUMPTION := "mars/telemetry/power_consumption",
    T_AIRLOCK := "mars/telemetry/airlock"
]

TOPIC_POWER_V1 = (T_SOLAR_ARRAY, T_POWER_BUS, T_POWER_CONSUMPTION)
TOPIC_ENVIRONMENT_V1 = (T_RADIATION, T_LIFE_SUPPORT)
TOPIC_THERMAL_LOOP_V1 = (T_THERMAL_LOOP)
TOPIC_AIRLOCK_V1 = (T_AIRLOCK)


REST_SENSORS = [
    "greenhouse_temperature",
    "entrance_humidity",
    "co2_hall",
    "hydroponic_ph",
    "water_tank_level",
    "corridor_pressure"
    "air_quality_pm25",
    "air_quality_voc"
]

async def consume_topic(topic):
    uri = f"ws://simulator:8080/api/telemetry/ws?topic={topic}"
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                print(f"[*] Sottoscritto al topic: {topic}")
                while True:
                    message = await websocket.recv()
                    data = json.loads(message)
                    
                    # unify the data
                    # send the data

                    print("Stream telemetry:")
                    print(f"[{topic}] Dati: {data}")
        except Exception as e:
            print(f"[!] Errore su {topic}: {e}. Riprovo tra 5s...")
            await asyncio.sleep(5)


async def poll_rest(sensor):
    uri = f"http://simulator:8080/api/sensors/{sensor}"
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(uri) as response:
                    data = await response.json()
                    
                    # unify the data
                    # send the data

                    print("Rest sensor:")
                    print(f"[{sensor}] Dati: {data}")

            except Exception as e:
                print(f"[!] REST error on {sensor}: {e}")
            await asyncio.sleep(5)



async def main():
    await asyncio.gather(
            *(consume_topic(t) for t in TOPICS),
            *(poll_rest(s) for s in REST_SENSORS)
        )

if __name__ == "__main__":
    data_example = {
        "timestamp": "2026-03-06T11:45:00",
        "rest_sensors": {"greenhouse_temp": 22.5, "co2_hall": 450},
        "telemetry": {"topic_alpha": 10.2}
    }

    messaging.send_message(data_example)

    asyncio.run(main())