import pandas as pd
import logging
import re
import apache_beam as beam
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

from google.cloud.sql.connector import connector


class UserOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument("--table_name", type=str)
        parser.add_value_provider_argument("--schema", type=str)
        parser.add_value_provider_argument("--hostname", type=str)
        parser.add_value_provider_argument("--port", type=str)
        parser.add_value_provider_argument("--username", type=str)
        parser.add_value_provider_argument("--password", type=str)
        parser.add_value_provider_argument("--database", type=str)
        parser.add_value_provider_argument("--sslmode", type=str)
        parser.add_value_provider_argument("--sslrootcert", type=str)
        parser.add_value_provider_argument("--sslcert", type=str)
        parser.add_value_provider_argument("--sslkey", type=str)
        parser.add_value_provider_argument("--app", type=str)
        parser.add_value_provider_argument("--ssl_certs_db", type=str)
        parser.add_value_provider_argument("--output_raw", type=str)
        parser.add_value_provider_argument("--sql_query", type=str)
        parser.add_value_provider_argument("--engine_wrapper", type=str)
        
pipeline_options = PipelineOptions()
user_options = pipeline_options.view_as(UserOptions)

class Md5Operation(beam.DoFn):
    def process(self, element):
        import base64
        gen_md5 = lambda x: base64.b64encode(hashlib.sha1(x).digest())
        element['md5_llave'] = gen_md5(element['concat_md5llave'].encode('UTF-8'))
        element['md5_completo'] = gen_md5(element['concat_md5completo'].encode('UTF-8'))
        del element['concat_md5llave']
        del element['concat_md5completo']
        return [element]

def ConfigSSLCerts():
    OScommand = [f'mkdir {user_options.app.get()}',
                f'gsutil cp {user_options.ssl_certs_db.get()}* {user_options.app.get()}/',
                f'chmod 600 {user_options.app.get()}/*']
    for command in OScommand:
        os.system(command)

def GetParquetSchema():
    ConfigSSLCerts()
    if user_options.engine_wrapper.get() == 'PostgresWrapper':
        conn = connector.connect("poised-rock-312023:us-central1:hg-bd-postgres",
                         "pg8000",
                         user="fkdjqwqlmwdsadsdwh",password="jhruhimcjdigrndidcie",
                         db="ytSalesLead")    

        # conn = psycopg2.connect(host = user_options.hostname.get(), 
        #                         user = user_options.username.get(), 
        #                         password = user_options.password.get(), 
        #                         dbname = user_options.database.get(), 
        #                         sslmode = user_options.sslmode.get(), 
        #                         sslrootcert = f"{user_options.app.get()}/{user_options.sslrootcert.get()}", 
        #                         sslcert = f"{user_options.app.get()}/{user_options.sslcert.get()}", 
        #                         sslkey = f"{user_options.app.get()}/{user_options.sslkey.get()}")

        info_query = f"""
        SELECT column_name, udt_name 
        FROM (
            SELECT column_name,udt_name,ordinal_position 
            FROM information_schema.columns 
            WHERE table_name='{user_options.table_name.get()}' 
            AND table_schema='{user_options.schema.get()}' 
            UNION 
            SELECT 'md5_llave' AS column_name, 'varchar' AS udt_name, 2000 as ordinal_position 
            UNION 
            SELECT 'md5_completo' AS column_name, 'varchar' AS udt_name, 2001 as ordinal_position 
            UNION 
            SELECT 'audit_usuario' AS column_name, 'varchar' AS udt_name, 2002 as ordinal_position 
            UNION 
            SELECT 'fecha_alta_gcp' AS column_name, 'timestamp' AS udt_name, 2003 as ordinal_position 
            UNION 
            SELECT 'fecha_ult_modificacion_gcp' AS column_name, 'timestamp' AS udt_name, 2004 as ordinal_position)
            AS DATA
            ORDER BY ordinal_position"""
        table_details = pd.read_sql_query(info_query,conn)
        data_type_mapping = {
            'varchar': pyarrow.string(),
            'bool': pyarrow.bool_(),
            'int8': pyarrow.int64(),
            'int4': pyarrow.int64(),
            'date': pyarrow.date32(),
            'time': pyarrow.time32('s'),
            'timetz': pyarrow.time32('s'),
            'numeric': pyarrow.int64(),
            'timestamp': pyarrow.timestamp(unit='s'),
            'timestamptz': pyarrow.timestamp(unit='s'),
            'text': pyarrow.string(),
            'float8': pyarrow.float64(),
        }
        parquet_schema = pyarrow.schema([])
        for f in table_details.itertuples():
            parquet_schema = parquet_schema.append(pyarrow.field(f.column_name, data_type_mapping[f.udt_name]))
        return parquet_schema
    elif user_options.engine_wrapper.get() == 'MySQLWrapper':
        ConfigSSLCerts()
        conn = pymysql.connect(host = user_options.hostname.get(),
                               user = user_options.username.get(),
                               passwd = user_options.password.get(),
                               db = user_options.database.get(),
                               ssl_ca = f"{user_options.app.get()}/{user_options.sslrootcert.get()}", 
                               ssl_cert = f"{user_options.app.get()}/{user_options.sslcert.get()}", 
                               ssl_key = f"{user_options.app.get()}/{user_options.sslkey.get()}")
        info_query_mysql = f"""SELECT column_name, data_type from 
        (SELECT column_name, data_type, ordinal_position 
        FROM information_schema.columns 
        WHERE table_name='{user_options.table_name.get()}' 
        AND table_schema='{user_options.schema.get()}' 
        UNION 
        SELECT 'md5_llave' AS column_name, 'char' AS data_type , 2000 as ordinal_position 
        UNION 
        SELECT 'md5_completo' AS column_name, 'char' AS data_type , 2001 as ordinal_position 
        UNION 
        SELECT 'audit_usuario' AS column_name, 'char' AS data_type , 2002 as ordinal_position 
        UNION 
        SELECT 'fecha_alta_gcp' AS column_name, 'timestamp' AS data_type, 2003 as ordinal_position 
        UNION 
        SELECT 'fecha_ult_modificacion_gcp' AS column_name, 'timestamp' AS data_type, 2004 as ordinal_position) 
        AS X order by ordinal_position""" 
        table_details=pd.read_sql_query(info_query_mysql,conn)
        data_type_mapping = {
            'varchar': pyarrow.string(),
            'char': pyarrow.string(),
            'bool': pyarrow.bool_(),
            'int': pyarrow.int64(),
            'tinyint': pyarrow.int64(),
            'date': pyarrow.date64(),
            'time': pyarrow.time64('us'),
            'numeric': pyarrow.decimal128(12,5),
            'timestamp': pyarrow.timestamp(unit='s'),
            'text': pyarrow.string(),
            'float': pyarrow.float64(),
            'json': pyarrow.string(),
            'double': pyarrow.float64(),
        }
        parquet_schema = pyarrow.schema([])
        for f in table_details.itertuples():
            parquet_schema = parquet_schema.append(pyarrow.field(f.column_name, data_type_mapping[f.data_type]))
        return parquet_schema
    
with beam.Pipeline(options=pipeline_options) as p:
    data = (p | 'Read from Postgres' >> ReadFromSQL(host = user_options.hostname.get(), 
                                                    port = user_options.port.get(),
                                                    username = user_options.username.get(), 
                                                    password = user_options.password.get(),
                                                    database = user_options.database.get(),
                                                    sslmode = user_options.sslmode.get(),
                                                    sslrootcert = f"{user_options.app.get()}/{user_options.sslrootcert.get()}", 
                                                    sslcert = f"{user_options.app.get()}/{user_options.sslcert.get()}", 
                                                    sslkey = f"{user_options.app.get()}/{user_options.sslkey.get()}",
                                                    certs = user_options.ssl_certs_db.get(),
                                                    query = user_options.sql_query,
                                                    wrapper = eval(user_options.engine_wrapper.get()),
                                                    app = user_options.app.get()))
#    result = data | beam.ParDo(Md5Operation())
#     result | "print" >> beam.Map(print)
#    result | WriteToParquet(file_path_prefix=user_options.output_raw,schema=GetParquetSchema(),file_name_suffix='.parquet')
    
    
