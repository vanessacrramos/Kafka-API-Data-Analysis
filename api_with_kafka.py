import time
import json
import requests
from kafka import KafkaProducer

def call_api(url):
    response = requests.get(url)
    try:
        if (response.status_code == 200):
            return response.json()
        else: 
            return 'Failed to retrieve data:', response.status_code
    except Exception as e:
        return 'Error:', e


KAFKA_TOPIC = 'topic_bitcoin_prices'
KAFKA_SERVER = 'localhost:9092'

producer = KafkaProducer (
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer= lambda x: json.dumps(x).encode('utf-8') 
)

def poll_ipa(url):
    while True:
        api_response_value = call_api(url)
        producer.send(KAFKA_TOPIC, value=api_response_value)
        
        time.sleep(10) #fetch/send data and then wait 10 seconds

poll_ipa('https://blockchain.info/ticker')