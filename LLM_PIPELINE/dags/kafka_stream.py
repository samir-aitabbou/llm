import json
import subprocess
import sys
from datasets import load_dataset
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

try:
    from kafka import KafkaProducer
except ImportError:
    print("Kafka library is not installed. Installing...")
    install("kafka-python")

def format_humaneval_data(item):
    """
    Format data from an item of the HumanEvalPack dataset.
    """
    data = {
        'task_id': item['task_id'],
        'prompt': item['prompt'],
        'declaration': item['declaration'],
        'canonical_solution': item['canonical_solution'],
        'buggy_solution': item['buggy_solution'],
        'bug_type': item['bug_type'],
        'failure_symptoms': item['failure_symptoms'],
        'entry_point': item['entry_point'],
        'import_': item.get('import', ''),
        'test_setup': item.get('test_setup', ''),
        'test': item['test'],
        'example_test': item['example_test'],
        'signature': item['signature'],
        'docstring': item['docstring'],
        'instruction': item['instruction']
    }
    return data

def send_to_kafka(data, topic='human_eval_data'):
    """
    Send formatted data to a specified Kafka topic.
    """
    # henaaaaaaaaa bedlttt  localhost:9092 --> kafka-broker:29092
    producer = KafkaProducer(bootstrap_servers=['kafka-broker:29092'],
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    try:
        producer.send(topic, data)
        producer.flush()
        print("Data sent to Kafka successfully")
    except Exception as e:
        print(f"Failed to send data to Kafka due to: {e}")


def stream_humaneval_data(language='python'):
    """
    Load items from the HumanEvalPack dataset, format them, and send them to Kafka.
    """
    ds = load_dataset("bigcode/humanevalpack", language, trust_remote_code=True)["test"]

    for item in ds:
        formatted_data = format_humaneval_data(item)
        send_to_kafka(formatted_data)