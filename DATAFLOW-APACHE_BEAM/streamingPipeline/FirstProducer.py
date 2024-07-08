import csv
import time
import os
from google.cloud import pubsub_v1

serviceAccount = "./CloudApiKey.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

topico = 'projects/dataflow-beam-428203/topics/MeusVoos'
publisher = pubsub_v1.PublisherClient()

entrada = "./voos_sample.csv"

with open(entrada, 'rb') as file:
    for row in file:
        print('Publishing in topic')
        publisher.publish(topico, row)
        time.sleep(2)