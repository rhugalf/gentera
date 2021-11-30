import pandas as pd
import logging
import re
import requests
import apache_beam as beam
from apache_beam.io.textio import WriteToText
from apache_beam.io.fileio import WriteToFiles
from apache_beam.pvalue import AsSingleton
from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.parquetio import WriteToParquet
from sqlssl.sql import ReadFromSQL
from sqlssl.wrapper import PostgresWrapper
from sqlssl.wrapper import MySQLWrapper
import psycopg2
import pymysql
import os
import google.cloud.logging
import json
import gcsfs
import pyarrow
import datetime
from datetime import date, timedelta
import hashlib
from hashlib import md5
import base64
import time
from datetime import datetime
import argparse
import requests
import ast

class UserOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument("--table_name", type=str, default='')
        parser.add_value_provider_argument("--schema", type=str, default='')
        parser.add_value_provider_argument("--hostname", type=str)
        parser.add_value_provider_argument("--port", type=str)
        ###parser.add_value_provider_argument("--username", type=str)
        ###parser.add_value_provider_argument("--password", type=str)
        parser.add_value_provider_argument("--database", type=str)
        parser.add_value_provider_argument("--sslmode", type=str)
        parser.add_value_provider_argument("--sslrootcert", type=str)
        parser.add_value_provider_argument("--sslcert", type=str)
        parser.add_value_provider_argument("--sslkey", type=str)
        parser.add_value_provider_argument("--app", type=str)
        parser.add_value_provider_argument("--ssl_certs_db", type=str)
        parser.add_value_provider_argument("--temp_file", type=str)
        parser.add_value_provider_argument("--sql_where", type=str, default='')
        parser.add_value_provider_argument("--query_key", type=str, default='')
        parser.add_value_provider_argument("--query_full", type=str, default='')
        parser.add_value_provider_argument("--engine_wrapper", type=str)

pipeline_options = PipelineOptions()
user_options = pipeline_options.view_as(UserOptions)

def get_function_usr() -> str:
    url=f"https://us-central1-poised-rock-312023.cloudfunctions.net/get-usr"
    r=requests.get(url)
    return r.text

def get_function_psw() -> str:
    url=f"https://us-central1-poised-rock-312023.cloudfunctions.net/get-psw"
    r=requests.get(url)
    return r.text

def showData (mesage) :
    print(mesage)

if user_options.engine_wrapper.get() == 'PostgresWrapper':
    # Postgres
    user = "'SII' AS audit_usuario"
    create_date = "CURRENT_TIMESTAMP AS fecha_alta_gcp"
    modify_date = "CURRENT_TIMESTAMP AS fecha_ult_modificacion_gcp"
elif user_options.engine_wrapper.get() == 'MySQLWrapper':
    # MySQL
    user = "'SII' AS audit_usuario"
    create_date = "CURRENT_TIMESTAMP() AS fecha_alta_gcp"
    modify_date = "CURRENT_TIMESTAMP() AS fecha_ult_modificacion_gcp"

class TextPrepPostgreSQL(beam.DoFn):
    # Postgres
    def process(self, element):
        result = []
        if element['udt_name'] == 'varchar':
            result.append('"'+element['column_name']+'"')
        else:
            result.append('CAST("'+element['column_name']+'" AS varchar(40))')
        return result

class TextPrepMySQL(beam.DoFn):
    # MySQL
    def process(self, element):
        result = []
        if element['udt_name'] == 'varchar':
            result.append('COALESCE('+element['column_name']+',"")')
        else:
            result.append('CAST(COALESCE('+element['column_name']+',"") AS char(40))')
        return result
    
class ConcatText1(beam.CombineFn):
    def create_accumulator(self):
        return []
    def add_input(self, accumulator, input):
        return (*accumulator,input)
    def merge_accumulators(self, accumulators):
        x = [*accumulators]
        x = ','.join(x[0])
        return x
    def extract_output(self, y):
        return 'CONCAT('+y+') AS concat_md5completo'

class ConcatText2(beam.CombineFn):
    def create_accumulator(self):
        return []
    def add_input(self, accumulator, input):
        return (*accumulator,input)
    def merge_accumulators(self, accumulators):
        x = [*accumulators]
        x = ','.join(x[0])
        return x
    def extract_output(self, y):
        if len(y)>0:
            return 'CONCAT('+y+') AS concat_md5llave'
        else:
            return "CONCAT('NA') AS concat_md5llave"

class ConcatText3(beam.CombineFn):
    def create_accumulator(self):
        return []
    def add_input(self, accumulator, input):
        return (*accumulator,input)
    def merge_accumulators(self, accumulators):
        x = [*accumulators]
        x = ','.join(x[0])
        return x
    def extract_output(self, y):
        return y

def TextFuzionPostgreSQL(element, key_part, full_part, schema_part, table_name_part, where_part):
    if element=="SELECT DISTINCT * ,":
        return element+key_part+', '+full_part
    else:
        return f'{element}{schema_part}."{table_name_part}" {where_part}'
    
def TextFuzionMySQL(element, key_part, full_part, schema_part, table_name_part, where_part):
    if element=="SELECT DISTINCT * ,":
        return element+key_part+', '+full_part
    else:
        return f'{element}{schema_part}.{table_name_part} {where_part}'
    
class ReadInputCustom(beam.DoFn):
    def __init__(self, input_param):
        self.input_param = input_param

    def process(self, unused_param):
        yield self.input_param.get()

if user_options.engine_wrapper.get() == 'PostgresWrapper':
    
    with beam.Pipeline(options=pipeline_options) as p:             
        user = get_function_usr()    
        password  = get_function_psw()

        where_stament = (p | "Preparation where" >> beam.Create([' '])
                           |  "Read where" >> beam.ParDo(ReadInputCustom(user_options.sql_where))
                        )
        md5_full = (p | "Read metadata full" >> ReadFromSQL(host = user_options.hostname.get(), 
                                                        port = user_options.port.get(),
                                                        username = user, #user_options.username.get(), 
                                                        password = password, #user_options.password.get(),
                                                        database = user_options.database.get(),
                                                        sslmode = user_options.sslmode.get(),
                                                        sslrootcert = f"{user_options.app.get()}/{user_options.sslrootcert.get()}", 
                                                        sslcert = f"{user_options.app.get()}/{user_options.sslcert.get()}", 
                                                        sslkey = f"{user_options.app.get()}/{user_options.sslkey.get()}",
                                                        certs = user_options.ssl_certs_db.get(),
                                                        query = user_options.query_full,
                                                        wrapper = eval(user_options.engine_wrapper.get()),
                                                        app = user_options.app.get()
                                                           )
                        | "Preparation full" >> beam.ParDo(TextPrepPostgreSQL())
                        | "Concatenate full" >> beam.CombineGlobally(ConcatText1())
               )
        # md5_key = (p | "Read metadata key" >> ReadFromSQL(host = user_options.hostname.get(), 
        #                                                 port = user_options.port.get(),
        #                                                 username = us, #user_options.username.get(), 
        #                                                 password = pss, #user_options.password.get(),
        #                                                 database = user_options.database.get(),
        #                                                 sslmode = user_options.sslmode.get(),
        #                                                 sslrootcert = f"{user_options.app.get()}/{user_options.sslrootcert.get()}", 
        #                                                 sslcert = f"{user_options.app.get()}/{user_options.sslcert.get()}", 
        #                                                 sslkey = f"{user_options.app.get()}/{user_options.sslkey.get()}",
        #                                                 certs = user_options.ssl_certs_db.get(),
        #                                                 query = user_options.query_key,
        #                                                 wrapper = eval(user_options.engine_wrapper.get()),
        #                                                 app = user_options.app.get())
        #                 | "Preparation key" >> beam.ParDo(TextPrepPostgreSQL())
        #                 | "Concatenate key" >> beam.CombineGlobally(ConcatText2())
        #        )
        # schema_str = (p | "Preparation schema" >> beam.Create([' '])
        #                    |  "Read shcema" >> beam.ParDo(ReadInputCustom(user_options.schema))
        #                 )
        # table_name_str = (p | "Preparation table name" >> beam.Create([' '])
        #                    |  "Read table name" >> beam.ParDo(ReadInputCustom(user_options.table_name))
        #                 )
        # final_query = (p   | "Create select" >> beam.Create([
        #                             "SELECT DISTINCT * ,",
        #                             f'''{user},{create_date},{modify_date} FROM '''
        #                          ])
        #                    | "Integration" >> beam.Map(TextFuzionPostgreSQL, 
        #                                                     key_part=AsSingleton(md5_key), 
        #                                                     full_part=AsSingleton(md5_full), 
        #                                                     schema_part=AsSingleton(schema_str),
        #                                                     table_name_part=AsSingleton(table_name_str),
        #                                                     where_part=AsSingleton(where_stament)
        #                                                    )
        #                    | "Full build query" >> beam.CombineGlobally(ConcatText3()).without_defaults()
        #                    | "Query file" >> WriteToText(user_options.temp_file,'.txt',num_shards=1)
        #               )
        
elif user_options.engine_wrapper.get() == 'MySQLWrapper':




    with beam.Pipeline(options=pipeline_options) as p:  #Código original


    

        where_stament = (p | "Preparation where" >> beam.Create([' '])
                           |  "Read where" >> beam.ParDo(ReadInputCustom(user_options.sql_where))
                        )
        md5_full = (p | "Read metadata full" >> ReadFromSQL(host = user_options.hostname.get(), 
                                                        port = user_options.port.get(),
                                                        username = user_options.username.get(), 
                                                        password = user_options.password.get(),
                                                        database = user_options.database.get(),
                                                        sslmode = user_options.sslmode.get(),
                                                        sslrootcert = f"{user_options.app.get()}/{user_options.sslrootcert.get()}", 
                                                        sslcert = f"{user_options.app.get()}/{user_options.sslcert.get()}", 
                                                        sslkey = f"{user_options.app.get()}/{user_options.sslkey.get()}",
                                                        certs = user_options.ssl_certs_db.get(),
                                                        query = user_options.query_full,
                                                        wrapper = eval(user_options.engine_wrapper.get()),
                                                        app = user_options.app.get()
                                                           )
                        | "Preparation full" >> beam.ParDo(TextPrepMySQL())
                        | "Concatenate full" >> beam.CombineGlobally(ConcatText1())
               )

        """
        md5_key = (p | "Read metadata key" >> ReadFromSQL(host = user_options.hostname.get(), 
                                                        port = user_options.port.get(),
                                                        username = user_options.username.get(), 
                                                        password = user_options.password.get(),
                                                        database = user_options.database.get(),
                                                        sslmode = user_options.sslmode.get(),
                                                        sslrootcert = f"{user_options.app.get()}/{user_options.sslrootcert.get()}", 
                                                        sslcert = f"{user_options.app.get()}/{user_options.sslcert.get()}", 
                                                        sslkey = f"{user_options.app.get()}/{user_options.sslkey.get()}",
                                                        certs = user_options.ssl_certs_db.get(),
                                                        query = user_options.query_key,
                                                        wrapper = eval(user_options.engine_wrapper.get()),
                                                        app = user_options.app.get())
                        | "Preparation key" >> beam.ParDo(TextPrepMySQL())
                        | "Concatenate key" >> beam.CombineGlobally(ConcatText2())
               )
        """
        
        with gcsfs.GCSFileSystem().open('gs://data-source-hg/TPM/config/PAR_SII_BCH_ING_TPM_RAW_ANTIGUEDADES.json', encoding='utf-8') as file:
            json_param = json.load(file)
            
        us = json_param["username"]    
        pss = json_param["password"]
        
        md5_key = (p | "Read metadata key" >> ReadFromSQL(host = user_options.hostname.get(), 
                                                        port = user_options.port.get(),
                                                        username = us, # p.options.username,  #user_options.username.get(), 
                                                        password = pss, # p.options.password,  #user_options.password.get(),
                                                        database = user_options.database.get(),
                                                        sslmode = user_options.sslmode.get(),
                                                        sslrootcert = f"{user_options.app.get()}/{user_options.sslrootcert.get()}",
                                                        #sslrootcert = f"{p.options.app}/{p.options.sslrootcert}",
                                                        sslcert = f"{user_options.app.get()}/{user_options.sslcert.get()}", 
                                                        #sslcert = f"{p.options.app}/{p.options.sslcert}",
                                                        sslkey = f"{user_options.app.get()}/{user_options.sslkey.get()}",
                                                        #sslkey = f"{p.options.app}/{p.options.sslkey}",
                                                        certs = user_options.ssl_certs_db.get(),
                                                        #certs = p.options.ssl_certs_db,
                                                        query = user_options.query_key,
                                                        wrapper = eval(user_options.engine_wrapper.get()),
                                                        app = user_options.app.get())
                        | "Preparation key" >> beam.ParDo(TextPrepMySQL())
                        | "Concatenate key" >> beam.CombineGlobally(ConcatText2())
               )
        

        schema_str = (p | "Preparation schema" >> beam.Create([' '])
                           |  "Read shcema" >> beam.ParDo(ReadInputCustom(user_options.schema))
                        )
        table_name_str = (p | "Preparation table name" >> beam.Create([' '])
                           |  "Read table name" >> beam.ParDo(ReadInputCustom(user_options.table_name))
                        )
        final_query = (p   | "Create select" >> beam.Create([
                                    "SELECT DISTINCT * ,",
                                    f'''{user},{create_date},{modify_date} FROM '''
                                 ])
                           | "Integration" >> beam.Map(TextFuzionMySQL, 
                                                            key_part=AsSingleton(md5_key), 
                                                            full_part=AsSingleton(md5_full), 
                                                            schema_part=AsSingleton(schema_str),
                                                            table_name_part=AsSingleton(table_name_str),
                                                            where_part=AsSingleton(where_stament)
                                                           )
                           | "Full build query" >> beam.CombineGlobally(ConcatText3()).without_defaults()
                           | "Query file" >> WriteToText(user_options.temp_file,'.txt',num_shards=1)
                      )
