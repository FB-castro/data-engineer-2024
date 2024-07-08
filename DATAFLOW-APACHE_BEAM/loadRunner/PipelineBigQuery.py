import apache_beam as beam
import os
import logging
from apache_beam.options.pipeline_options import PipelineOptions

logging.basicConfig(level=logging.INFO)

pipeline_options = {
    'project': "dataflow-beam-428203",
    'runner': 'DataflowRunner',
    'region': 'us-east1',
    'staging_location': 'gs://apachebeam-data/temp',
    'temp_location': 'gs://apachebeam-data/temp',
    'template_location': 'gs://apachebeam-data/template/batch_to_bigquery',
    'save_main_session': True
}

pipeline_options = PipelineOptions.from_dictionary(pipeline_options)

serviceAccount = "./CloudApiKey.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

p1 = beam.Pipeline(options=pipeline_options)

class Filtro(beam.DoFn):
    def process(self, record):
        try:
            if int(record[8]) > 0:
                return [record]
        except Exception as e:
            logging.error(f"Error in Filtro.process: {e} with record: {record}")
            return []

def criar_dict_nivel1(record):
    try:
        return {'airport': record[0], 'lista': record[1]}
    except Exception as e:
        logging.error(f"Error in criar_dict_nivel1: {e} with record: {record}")
        return {}

def desaninhar_dict(record):
    try:
        def expand(key, value):
            if isinstance(value, dict):
                return [(key + '_' + k, v) for k, v in desaninhar_dict(value).items()]
            else:
                return [(key, value)]
        items = [item for k, v in record.items() for item in expand(k, v)]
        return dict(items)
    except Exception as e:
        logging.error(f"Error in desaninhar_dict: {e} with record: {record}")
        return {}

def criar_dict_nivel0(record):
    try:
        return {
            'airport': record['airport'],
            'qtd_airport_delay': record.get('lista_Qtd_Atrasos', [0])[0],
            'delay_time': record.get('lista_Tempo_Atrasos', [0])[0]
        }
    except Exception as e:
        logging.error(f"Error in criar_dict_nivel0: {e} with record: {record}")
        return {}

table_schema = 'airport:STRING, qtd_airport_delay:INTEGER, delay_time:INTEGER'
table = 'dataflow-beam-428203.dataFlow_flights.dataflow_flights_delay'

Tempo_Atrasos = (
    p1
    | "Importar Dados Atraso" >> beam.io.ReadFromText(r"gs://apachebeam-data/entrada/voos_sample.csv", skip_header_lines=1)
    | "Separar por Vírgulas Atraso" >> beam.Map(lambda record: record.split(','))
    | "Pegar voos com atraso" >> beam.ParDo(Filtro())
    | "Criar par atraso" >> beam.Map(lambda record: (record[4], int(record[8])))
    | "Somar por key" >> beam.CombinePerKey(sum)
)

Qtd_Atrasos = (
    p1
    | "Importar Dados" >> beam.io.ReadFromText(r"gs://apachebeam-data/entrada/voos_sample.csv", skip_header_lines=1)
    | "Separar por Vírgulas Qtd" >> beam.Map(lambda record: record.split(','))
    | "Pegar voos com Qtd" >> beam.ParDo(Filtro())
    | "Criar par Qtd" >> beam.Map(lambda record: (record[4], int(record[8])))
    | "Contar por key" >> beam.combiners.Count.PerKey()
)

tabela_atrasos = (
    {'Qtd_Atrasos': Qtd_Atrasos, 'Tempo_Atrasos': Tempo_Atrasos}
    | "CoGroupByKey" >> beam.CoGroupByKey()
    | "Criar Dict Nivel 1" >> beam.Map(lambda record: criar_dict_nivel1(record))
    | "Desaninhar Dict" >> beam.Map(lambda record: desaninhar_dict(record))
    | "Criar Dict Nivel 0" >> beam.Map(lambda record: criar_dict_nivel0(record))
    | "Write To BigQuery" >> beam.io.WriteToBigQuery(
        table,
        schema=table_schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        custom_gcs_temp_location='gs://apachebeam-data/temp'
    )
)

logging.info("Starting the pipeline.")
p1.run().wait_until_finish()
logging.info("Pipeline completed.")
