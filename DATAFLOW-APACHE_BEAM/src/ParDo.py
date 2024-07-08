import apache_beam as beam


p1 = beam.Pipeline()

class filtro(beam.DoFn):
    def process(self, record):
        if int(record[8]) > 0:
            return [record]


Tempo_Atraso = (
    p1
    | "Import Data" >> beam.io.ReadFromText(r"voos_sample.csv", skip_header_lines=1)
    | "Split data" >> beam.Map(lambda record: record.split(','))
    | "Delay Flights" >> beam.ParDo(filtro())
    | "Create pair" >> beam.Map(lambda record: (record[4], int(record[8]) ) )
    | "Sum by key" >> beam.CombinePerKey(sum)
)


Qtd_Atrasos = (
    p1
    | "QTD Import Data" >> beam.io.ReadFromText(r"voos_sample.csv", skip_header_lines= 1)
    | "QTD Split data" >> beam.Map(lambda record: record.split(','))
    | "QTD Delay Flights" >> beam.ParDo(filtro())
    | "QTD Create pair" >> beam.Map(lambda record: (record[4], int(record[8]) ) )
    | "QTD Count by key" >> beam.combiners.Count.PerKey()
)

Tabela_delay = (
    {'QTD_atrasos' : Qtd_Atrasos, 'Tempo_atrasos': Tempo_Atraso}
    | "QTD Group by" >> beam.CoGroupByKey()
    | beam.Map(print)
)

p1.run()