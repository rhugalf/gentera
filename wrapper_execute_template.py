from googleapiclient.discovery import build
from google.cloud import storage
import gcsfs
import sys
import json
import datetime

print('HOLA')

# Recibe un JSON con los parametros
json_gs = sys.argv[2]
exe_param = sys.argv[1]

with gcsfs.GCSFileSystem().open(json_gs) as f:
    jd = json.load(f)
    print("File json: ", jd)

try:
    # Datos de RAW
    url_raw = jd['url_raw']

    # Datos de TRN
    job_name_trn = jd['job_name_trn']
    url_trn = jd['url_trn']
    file_name_trn = jd['file_name_trn']
    template_location_trn = jd['template_location_trn']

    # Datos de BQ
    job_name_bq = jd['job_name_bq']
    table_id = jd['table_id']
    template_location_bq = jd['template_location_bq']
    
    # Datos de CC Fin RAW
    job_name_cc = jd['job_name_cc']
    template_location_raw_cc = jd['template_location_raw_cc']
    
    # Datos de CC Fin TRN
    job_name_cc_trn = jd['job_name_cc_trn']
    template_location_trn_cc = jd['template_location_trn_cc']
    
except:
    pass
    
# Datos Generales para la ejecucion
temp_location = jd['temp_location']
project = jd['project']
region = jd['region']
# subnetwork = jd['subnetwork']
service_account_email = jd['service_account_email']
machine_type = jd['machine_type']
max_num_workers = jd['max_num_workers']
num_workers = jd['num_workers']

print("exe_param: " , exe_param)

### SQL GENERATOR ###
if exe_param.lower()=='sql':
    print("iniciando SQL GENERATOR")
    table_name = jd['table_name']
    schema = jd['schema']
    repro_start = jd['fch_reproceso_ini']
    repro_end = jd['fch_reproceso_fin']
    edit_date = jd['ts_modificacion']
    create_date = jd['ts_alta']
    delta = jd['delta']
    app = jd['aplicativo']
    url_app = jd['destinoraw']
    extract_type = jd['tipo_extraccion']
    target_table = jd['target_table']
    job_name_sql = jd['name_job_sql']
    template_location_genquery = jd['template_location_genquery']
    engine_wrapper = jd['engine_wrapper']
    if engine_wrapper == 'PostgresWrapper':
        query_key = f"""SELECT column_name, udt_name 
                        FROM information_schema.columns 
                        NATURAL LEFT JOIN information_schema.key_column_usage 
                        WHERE table_name='{table_name}' 
                        AND table_schema='{schema}' 
                        AND RIGHT(constraint_name,4)='pkey'"""
        query_full = f"""SELECT column_name, udt_name 
                            FROM information_schema.columns 
                            WHERE table_name='{table_name}' 
                            AND table_schema='{schema}' 
                            ORDER BY ordinal_position"""
    elif engine_wrapper == 'MySQLWrapper':
        query_key = f"""SELECT column_name AS column_name, data_type AS udt_name 
                        FROM information_schema.table_constraints tc 
                        JOIN information_schema.columns c
                        ON c.table_schema = tc.constraint_schema 
                        AND tc.table_name = c.table_name
                        WHERE constraint_type = 'PRIMARY KEY' 
                        AND tc.table_name = '{table_name}' 
                        AND c.table_schema='{schema}'"""
        query_full = f"""SELECT column_name AS column_name, data_type AS udt_name
                            FROM information_schema.columns 
                            WHERE table_name='{table_name}' 
                            AND table_schema='{schema}' 
                            ORDER BY ordinal_position"""
    temp_file = f"{url_app}sql_queries/sql-query-{app}-{target_table}"
    print("temp_file L 85")
    print(temp_file)
    if extract_type == 'F':
        date_execute = str(datetime.date.today())
        sql_where = ""
    elif extract_type == 'I':
        date_execute = str(datetime.date.today()-datetime.timedelta(days=int(delta)))
        sql_where = f"""WHERE CAST("{create_date}" AS DATE) = CAST('{date_execute}' AS DATE) OR CAST("{edit_date}" AS DATE) = CAST('{date_execute}' AS DATE)"""
    
    print("sql_where: " , sql_where)
    parameters = {
        'query_key': query_key,
        'query_full': query_full,
        'table_name': table_name,
        'schema': schema,
        'sql_where': sql_where,
        'temp_file': temp_file,
    }
    print(" parameters: " ,  parameters)
    
    environment_params = {
        'machineType': machine_type,
        'maxWorkers': max_num_workers,
        'numWorkers': num_workers,
        'serviceAccountEmail': service_account_email,
        #'subnetwork': subnetwork,
        'tempLocation': temp_location,
        'workerRegion': region,
    }
    print("environment_params: " ,  environment_params)
    
    #request = dataflow.projects().templates().launch(
    dataflow = build('dataflow', 'v1b3')
    print("dataflow: " ,  dataflow)
        
    request = dataflow.projects().locations().templates().launch(
        projectId=project,
        gcsPath=template_location_genquery,
        location=region,
        body={
            'jobName': job_name_sql+'-'+target_table.replace('_','-')+'-'+date_execute,
            'parameters': parameters,
            'environment': environment_params
        }
    )
    print("request: " ,  request)
    
    response = request.execute()
    print("response: " ,  response)
        
    print(response)

### CAPA RAW ###
if exe_param.lower()=='raw':
    delta = jd['delta']
    app = jd['aplicativo']
    url_app = jd['destinoraw']
    extract_type = jd['tipo_extraccion']
    target_table = jd['target_table']
    job_name_raw = jd['nombre_job']
    template_location_raw = jd['template_location_raw']
    temp_file = f"{url_app}sql_queries/sql-query-{app}-{target_table}-00000-of-00001.txt"
    print("temp_file")
    print(temp_file)
    if extract_type == 'F': 
        date_execute = str(datetime.date.today())
    elif extract_type == 'I':
        date_execute = str(datetime.date.today()-datetime.timedelta(days=int(delta)))
    output_raw = f"{url_app}{target_table}/{date_execute}/{target_table}"
    print("output_raw", output_raw)    
    
    with gcsfs.GCSFileSystem().open(temp_file) as f:
        sql_string = f.read().decode('utf-8')
    print("sql_string",  sql_string)    
    parameters = {
        'sql_query': sql_string,
        'output_raw': output_raw
    }
    print("parameters",  parameters) 
        
    environment_params = {
        'machineType': machine_type,
        'maxWorkers': max_num_workers,
        'numWorkers': num_workers,
        'serviceAccountEmail': service_account_email,
       # 'subnetwork': subnetwork,
        'tempLocation': temp_location,
        'workerRegion': region,
    }
    print("environment_params",  environment_params)
    #request = dataflow.projects().templates().launch(
    dataflow = build('dataflow', 'v1b3')
    print("inicia dataflow hghghhg...")    

    request = dataflow.projects().locations().templates().launch(
        projectId=project,
        gcsPath=template_location_raw,
        location=region,
        body={
            'jobName': job_name_raw+'-'+date_execute,
            'parameters': parameters,
            'environment': environment_params,
        }
    )
    response = request.execute()
    print(response)
    
### CAPA TRN ###
# Trata de leer la fecha de ayer en en origen RAW, calculada a partir de la fecha del sistema
if exe_param.lower()=='trn-delta':
#     Se captura la fecha a procesar
    date_execute = str(datetime.date.today()- datetime.timedelta(days=1))
#     Origen y Destino de la Operacion
    url_source = url_raw+date_execute+'/' # Se agrego la diagonal al path
    url_dest = url_trn+date_execute+'/'+file_name_trn
    #folder_date = date_execute  # Se agrego esta linea para CC
    print('url_source: ' + url_source)
    print('url_dest: ' + url_dest)
    print('folder_date: ' + date_execute)
    parameters = {
        'url_raw': url_source,
        'url_trn': url_dest,
        'folder_date': str(datetime.date.today()), # Se agrego esta linea para CC
    }
    environment_params = {
        'machineType': machine_type,
        'maxWorkers': max_num_workers,
        'numWorkers': num_workers,
        'serviceAccountEmail': service_account_email,
        'subnetwork': subnetwork,
        'tempLocation': temp_location,
        'workerRegion': region,
    }
    #request = dataflow.projects().templates().launch(
    dataflow = build('dataflow', 'v1b3')
    request = dataflow.projects().locations().templates().launch(
        projectId=project,
        gcsPath=template_location_trn,
        location=region,
        body={
            'jobName': job_name_trn+'-'+date_execute,
            'parameters': parameters,
            'environment': environment_params
        }
    )
    response = request.execute()
    print(response)
# itera por todas las fechas que encuentre en el origen RAW
elif exe_param.lower()=='trn-full':
    gs_raw_split = url_raw.split('/')
    bucket_name = gs_raw_split[2]
    prefix = gs_raw_split[3]+'/'+gs_raw_split[4]+'/'
    blobs = storage.Client().list_blobs(bucket_name, prefix=prefix, delimiter='/')
    blob_content = list(blobs)
    folders = list(blobs.prefixes)
    if len(folders)>0:
        for folder in folders:
            # Origen y Destino de la Operacion
            url_source = 'gs://'+bucket_name+'/'+folder
            url_dest = url_trn+folder.split('/')[2]+'/'+file_name_trn
            print('url_source: ' + url_source)
            print('url_dest: ' + url_dest)
            
            folder_date = folder.split('/')[2]
            print('folder_date: ' + folder_date) #TPM/raw_antiguedades/2021-11-17/
            parameters = {
                'url_raw': url_source,
                'url_trn': url_dest,
                'folder_date': folder_date,
            }
            environment_params = {
                'machineType': machine_type,
                'maxWorkers': max_num_workers,
                'numWorkers': num_workers,
                'serviceAccountEmail': service_account_email,
                'subnetwork': subnetwork,
                'tempLocation': temp_location,
                'workerRegion': region,
            }
            #request = dataflow.projects().templates().launch(
            dataflow = build('dataflow', 'v1b3')
            request = dataflow.projects().locations().templates().launch(
                projectId=project,
                gcsPath=template_location_trn,
                location=region,
                body={
                    'jobName': job_name_trn+'-'+folder.split('/')[2],
                    'parameters': parameters,
                    'environment': environment_params
                }
            )
            response = request.execute()
            print(response)
            
### CAPA BQ ###
# Trata de leer la fecha de ayer en en origen HOM, calculada a partir de la fecha del sistema
elif exe_param.lower()=='bq-delta':
#     Se captura la fecha a procesar
    date_execute = str(datetime.date.today()- datetime.timedelta(days=1))
#     Origen y Destino de la Operacion
    url_source = url_hom+date_execute
#     table_id = table_id
    print('url_source: ' + url_source)
    print('table_id: ' + table_id)
    parameters = {
        'url_hom': url_source,
        'table_id': table_id,
    }
    environment_params = {
        'machineType': machine_type,
        'maxWorkers': max_num_workers,
        'numWorkers': num_workers,
        'serviceAccountEmail': service_account_email,
        'subnetwork': subnetwork,
        'tempLocation': temp_location,
        'workerRegion': region,
    }
    #request = dataflow.projects().templates().launch(
    dataflow = build('dataflow', 'v1b3')
    request = dataflow.projects().locations().templates().launch(
        projectId=project,
        gcsPath=template_location_bq,
        location=region,
        body={
            'jobName': job_name_bq+'-'+date_execute,
            'parameters': parameters,
            'environment': environment_params
        }
    )
    response = request.execute()
    print(response)
# itera por todas las fechas que encuentre en el origen HOM
elif exe_param.lower()=='bq-full':
    gs_trn_split = url_trn.split('/')
    bucket_name = gs_trn_split[2]
    prefix = gs_trn_split[3]+'/'+gs_trn_split[4]+'/'
    blobs = storage.Client().list_blobs(bucket_name, prefix=prefix, delimiter='/')
    blob_content = list(blobs)
    folders = list(blobs.prefixes)
    if len(folders)>0:
        for folder in folders:
            # Origen y Destino de la Operacion
            url_source = 'gs://'+bucket_name+'/'+folder
            print('url_source: ' + url_source)
            print('table_id: ' + table_id)
            parameters = {
                'url_trn': url_source,
                'table_id': table_id,
            }
            environment_params = {
                'machineType': machine_type,
                'maxWorkers': max_num_workers,
                'numWorkers': num_workers,
                'serviceAccountEmail': service_account_email,
                'subnetwork': subnetwork,
                'tempLocation': temp_location,
                'workerRegion': region,
            }
            #request = dataflow.projects().templates().launch(
            dataflow = build('dataflow', 'v1b3')
            request = dataflow.projects().locations().templates().launch(
                projectId=project,
                gcsPath=template_location_bq,
                location=region,
                body={
                    'jobName': job_name_bq+'-'+folder.split('/')[2],
                    'parameters': parameters,
                    'environment': environment_params
                }
            )
            response = request.execute()
            print(response)

### CAPA RAW CC FIN ###
if exe_param.lower()=='raw-cc':
    job_name_cc = jd['job_name_cc']
    template_location_raw_cc = jd['template_location_raw_cc']
    date_execute = str(datetime.date.today())

    environment_params = {
        'machineType': machine_type,
        'maxWorkers': max_num_workers,
        'numWorkers': num_workers,
        'serviceAccountEmail': service_account_email,
        'subnetwork': subnetwork,
        'tempLocation': temp_location,
        'workerRegion': region,
    }
    print("environment_params",  environment_params)
        
    #request = dataflow.projects().templates().launch(
    dataflow = build('dataflow', 'v1b3')
    request = dataflow.projects().locations().templates().launch(
        projectId=project,
        gcsPath=template_location_raw_cc,
        location=region,
        body={
            'jobName': job_name_cc+'-'+date_execute,
            'environment': environment_params,
        }
    )
    response = request.execute()
    print(response)

    ### CAPA TRN CC FIN ###
if exe_param.lower()=='trn-delta-cc':
    job_name_cc_trn = jd['job_name_cc_trn']
    template_location_trn_cc = jd['template_location_trn_cc']
    date_execute = str(datetime.date.today())

    environment_params = {
        'machineType': machine_type,
        'maxWorkers': max_num_workers,
        'numWorkers': num_workers,
        'serviceAccountEmail': service_account_email,
        'subnetwork': subnetwork,
        'tempLocation': temp_location,
        'workerRegion': region,
    }
    print("environment_params",  environment_params)
        
    #request = dataflow.projects().templates().launch(
    dataflow = build('dataflow', 'v1b3')
    request = dataflow.projects().locations().templates().launch(
        projectId=project,
        gcsPath=template_location_trn_cc,
        location=region,
        body={
            'jobName': job_name_cc_trn+'-'+date_execute,
            'environment': environment_params,
        }
    )
    response = request.execute()
    print(response)
            
else:
    print('Parametro no reconocido')