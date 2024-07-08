import apache_beam as beam
import os
from apache_beam.options.pipeline_options import PipelineOptions

#template created

pipeline_options = {
    'project' : "dataflow-beam-428203",
    'runner' : 'DataflowRunner',
    'region' : 'us-central1',
    'staging_location' : 'gs://apachebeam-data/temp',
    'temp_location' : 'gs://apachebeam-data/temp',
    'template_location' : 'gs://apachebeam-data/template/BatchJob_dfVoos' 
}

pipeline_options = PipelineOptions.from_dictionary(pipeline_options)


serviceAccount = "./CloudApiKey.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

p1 = beam.Pipeline(options=pipeline_options)


class filtro(beam.DoFn):
    def process(self, record):
        if int(record[8]) > 0:
            return [record]


Tempo_Atraso = (
    p1
    | "Import Data" >> beam.io.ReadFromText("gs://apachebeam-data/entrada/voos_sample.csv", skip_header_lines=1)
    | "Split data" >> beam.Map(lambda record: record.split(','))
    | "Delay Flights" >> beam.ParDo(filtro())
    | "Create pair" >> beam.Map(lambda record: (record[4], int(record[8]) ) )
    | "Sum by key" >> beam.CombinePerKey(sum)
)


Qtd_Atrasos = (
    p1
    | "QTD Import Data" >> beam.io.ReadFromText("gs://apachebeam-data/entrada/voos_sample.csv", skip_header_lines= 1)
    | "QTD Split data" >> beam.Map(lambda record: record.split(','))
    | "QTD Delay Flights" >> beam.ParDo(filtro())
    | "QTD Create pair" >> beam.Map(lambda record: (record[4], int(record[8]) ) )
    | "QTD Count by key" >> beam.combiners.Count.PerKey()
)

Tabela_delay = (
    {'QTD_atrasos' : Qtd_Atrasos, 'Tempo_atrasos': Tempo_Atraso}
    | "QTD Group by" >> beam.CoGroupByKey()
    | "Deploy GCP" >> beam.io.WriteToText(f"gs://apachebeam-data/saida/Clean_File.csv")
)

p1.run()
