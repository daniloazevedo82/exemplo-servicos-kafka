from kafka import KafkaConsumer, TopicPartition
from time import sleep
import json

consumidor = KafkaConsumer(
    bootstrap_servers=["kafka:29092"], 
    auto_offset_reset='earliest', 
    consumer_timeout_ms=1000, 
    api_version=(0, 10, 1))

particao = TopicPartition("separacao", 0)
consumidor.assign([particao])

consumidor.seek_to_beginning(particao)
offset = 0
while True: 
    print("esperando mensagens...")    

    for dado in consumidor:
        offset = dado.offset

        processo = json.loads(dado.value)
        print("processo:", processo)

    consumidor.seek(particao, offset + 1)

# consumidor.close()