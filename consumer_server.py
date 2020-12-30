from json import loads
from kafka import KafkaConsumer

# https://knowledge.udacity.com/questions/396881
if __name__ == "__main__":
    consumer = KafkaConsumer(bootstrap_servers=['localhost:8082'],
                             auto_offset_reset='earliest',
                             group_id="consumer_server",
                             value_deserializer=lambda x: loads(x.decode('utf-8')))
    
    consumer.subscribe(['police-calls'])

    for message in consumer:
        print(message.value)
    
