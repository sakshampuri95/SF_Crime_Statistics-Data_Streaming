import producer_server
from pathlib import Path
import json

def run_kafka_server():
    # TODO get the json file path
    input_file = f"{Path(__file__).parents[0]}/police-department-calls-for-service.json"

    # TODO fill in blanks
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="com.police.dept.crime.stats",
        bootstrap_servers="localhost:9092",
        client_id="com.udacity.broker"
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()