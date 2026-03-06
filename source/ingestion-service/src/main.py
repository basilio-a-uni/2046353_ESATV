import messaging


if __name__ == "__main__":
    data_example = {
        "timestamp": "2026-03-06T11:45:00",
        "rest_sensors": {"greenhouse_temp": 22.5, "co2_hall": 450},
        "telemetry": {"topic_alpha": 10.2}
    }

    messaging.send_message(data_example)