import apache_beam as beam
import os
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import window

#Ler precisa fazer o decode(utf-8)
#Escrever fazer um encode(utf-8)

pipeline_options = {
    'project': "dataflow-beam-428203",
    'runner': 'DataflowRunner',
    'region': 'us-east1',
    'staging_location': 'gs://apachebeam-data/temp',
    'temp_location': 'gs://apachebeam-data/temp',
    'template_location': 'gs://apachebeam-data/template/streaming_sliding',
    'save_main_session': True,
    'streaming': True
}

pipeline_options = PipelineOptions.from_dictionary(pipeline_options)

serviceAccount = "./CloudApiKey.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

p1 = beam.Pipeline(options=pipeline_options)
subscription = 'projects/dataflow-beam-428203/subscriptions/MeusVoos-Sub'
saida = 'projects/dataflow-beam-428203/topics/saida'

class separar_linhas (beam.DoFn):
    def process(self, record):
        return [record.decode('utf=8').split(',') ]

class filtro(beam.DoFn):
    def process(self, record):
        if int(record[8]) > 0:
            return [record]

pcolletion_entrada = (
    p1 | 'Read from pubsub topic' >> beam.io.ReadFromPubSub(subscription=subscription)
)

Tempo_Atrasos = (
    pcolletion_entrada
    | "SEPARAR POR VÍRGULAS ATRASO" >> beam.ParDo(separar_linhas())
    | "PEGAR VOOS COM ATRASO" >> beam.ParDo(filtro())
    | "CRIAR PAR ATRASO" >> beam.Map(lambda record: (record[4], int(record[8]) ) )
    | "WINDOW" >> beam.WindowInto(window.SlidingWindows(10, 5) )  #Janela de 10 segundos a cada 5 segundos
    | "SOMAR POR KEY" >> beam.CombinePerKey(sum)
)

Qtd_Atrasos = (
    pcolletion_entrada
    | "SEPARAR POR VÍRGULA QTD" >> beam.ParDo(separar_linhas() )
    | "PEGAR VOOS COM QTD" >> beam.ParDo(filtro())
    | "CRIAR PAR QTD" >> beam.Map(lambda record: (record[4], int(record[8]) ) )
    | "WINDOW ATR" >> beam.WindowInto(window.SlidingWindows(10,5))
    | "CONTAR POR KEY" >> beam.combiners.Count.PerKey()
)

tabela_atrasos = (
    {'Qtd_Atrasos' : Qtd_Atrasos, 'Tempo_Atrasos' : Tempo_Atrasos}
    | "JOIN ATRASOS E QTD" >> beam.CoGroupByKey()
    | "CONVERTING TO BYTE STRING" >> beam.Map(lambda row: (''.join(str(row) ).encode('utf-8') ) )
    | "ESCREVER NO TÓPICO" >> beam.io.WriteToPubSub(saida)
)

result = p1.run()
result.wait_until_finish()