import re
import argparse
import logging
import requests
#from sqlssl.sql import ReadFromSQL
#from sqlssl.wrapper import PostgresWrapper
import apache_beam as beam
from pysql_beam.sql_io.sql import ReadFromSQL
from pysql_beam.sql_io.wrapper import PostgresWrapper
from apache_beam.options.pipeline_options import PipelineOptions

PROJECT='poised-rock-312023'
BUCKET='data-source-hg'

def lectura(line):
    print(line)

def parse_method(string_input):
    values=re.split(";",re.sub('\r\n', '', re.sub('"', '',string_input)))
    row=dict(
             zip(('pais','fecha_hora','referencia','moneda','monto'),
              values))
    print(row)
    return row

def run():
    argv=[
         '--project={0}'.format(PROJECT),
         '--job_name=cifrasjob',
         '--save_main_session',
         '--password=jhruhimcjdigrndidcie,'
         '--staging_location=gs://{0}/staging/'.format(BUCKET),
         '--temp_location=gs://{0}/staging/'.format(BUCKET),
         '--region=us-central1',
         '--runner=DataflowRunner'
         ]
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        help='Input file to read.',
        default='gs://inputs-hg/data_postgr.csv')

    parser.add_argument('--output',
                     dest='output',
                     required=False,
                     help='Output BQ table to write results to.',
                     default='indra.cifras2021')
    print("golfd pp")
    known_args,pipeline_args=parser.parse_known_args(argv)
    pipeline=beam.Pipeline(options=PipelineOptions(pipeline_args))

    (pipeline | 'ReadFromSQL' >> (ReadFromSQL(
                                                host = "35.239.58.233", 
                                                port = "5432",
                                                username = "fkdjqwqlmwdsadsdwh", 
                                                password = "jhruhimcjdigrndidcie",
                                                database = "ytSalesLead",
                                                wrapper=eval("PostgresWrapper"),
                                                batch=100))
    #           | 'ReadLine' >> beam.Map(lambda line:parse_method(line))
    #           | 'Write BigQuery' >> beam.io.Write(
    #                                                 beam.io.BigQuerySink(
    #                                                     known_args.output,
    #                                                     schema='pais:STRING,fecha_hora:STRING,referencia:STRING,moneda:STRING,monto:STRING',
    #                                                     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    #                                                     write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
    #                                                 )
    #                                              )
    )
    pipeline.run().wait_until_finish()


if __name__=='__main__':
    run()
