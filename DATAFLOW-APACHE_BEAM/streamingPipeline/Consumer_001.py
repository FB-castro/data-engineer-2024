import csv
import time
from google.cloud import pubsub_v1
import os


serviceAccount = "./CloudApiKey.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

subscription = 'projects/dataflow-beam-428203/subscriptions/MeusVoos-Sub'
subscriber = pubsub_v1.SubscriberClient()

def mostrar_msg(mensagem):
    print(('Mensagem: {}'.format(mensagem)))
    mensagem.ack()

subscriber.subscribe(subscription, callback=mostrar_msg)

while True:
    time.sleep(3)