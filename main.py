import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

options = PipelineOptions()


def run():
    p = beam.Pipeline(options=options)
    (p
     | beam.io.ReadFromText('HRDataset_v14.csv')
     | beam.Map(lambda x: x.split(','))
     | beam.Filter(lambda linha: linha[18]=='Single')
     #| beam.Map(print)
     | beam.io.WriteToText('solteiros', file_name_suffix='.csv'))

    p.run()

run()
