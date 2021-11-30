import gcsfs
import sys
import json
import datetime
import os
from shlex import quote

# Recibe un JSON con los parametros
json_gs = sys.argv[2]
exe_param = sys.argv[1]

with gcsfs.GCSFileSystem().open(json_gs, encoding='utf-8') as f:
    jd = json.load(f)

# Datos generales 
temp_location = jd['temp_location']
staging_location = jd['staging_location']
service_account_email = jd['service_account_email']
setup_file = jd['setup_file']
project = jd['project']
runner = jd['runner']
region = jd['region']


if exe_param.lower()=='sql':
    print('----Generando QUERY SQL GENERATOR----')
    # Datos de RAW
    table_name = jd['table_name']
    schema = jd['schema']
    hostname = jd['hostname']
    port = jd['port']
    username = jd['username']
    password = jd['password'].replace('$','\$')
    database = jd['database']
    sslmode = jd['sslmode']
    sslrootcert = jd['sslrootcert']
    sslcert = jd['sslcert']
    sslkey = jd['sslkey']
    engine_wrapper = jd['engine_wrapper']
    app = jd['aplicativo']
    ssl_certs_db = jd['certificados']
    app_url = jd['destinoraw']
    template_location_genquery = jd['template_location_genquery']
    code_file_genquery = jd['code_file_genquery']
#     Comando de creacion
    comand_string = f'''python3 {code_file_genquery} \
                        --setup_file {setup_file} \
                        --project {project} \
                        --save_main_session \
                        --runner {runner} \
                        --region {region} \
                        --temp_location {temp_location} \
                        --staging_location {staging_location} \
                        --template_location {template_location_genquery} \
                        --service_account_email {service_account_email} \
                        --hostname {hostname} \
                        --port {port} \
                        --username {username} \
                        --password "{password}" \
                        --database {database} \
                        --sslmode {sslmode} \
                        --sslrootcert {sslrootcert} \
                        --sslcert {sslcert} \
                        --sslkey {sslkey} \
                        --app {app} \
                        --ssl_certs_db {ssl_certs_db} \
                        --engine_wrapper {engine_wrapper}'''

#     # Ejecucion de comando
    os.system(comand_string)
    print("SQL comand_string: ", comand_string)


if exe_param.lower()=='raw':
    print('----Procesando RAW----')
    # Datos de RAW
    table_name = jd['table_name']
    schema = jd['schema']
    hostname = jd['hostname']
    port = jd['port']
    username = jd['username']
    password = jd['password'].replace('$','\$')
    database = jd['database']
    sslmode = jd['sslmode']
    sslrootcert = jd['sslrootcert']
    sslcert = jd['sslcert']
    sslkey = jd['sslkey']
    engine_wrapper = jd['engine_wrapper']
    
    app = jd['aplicativo']
    ssl_certs_db = jd['certificados']
    
    code_file_raw = jd['code_file_raw']
    template_location_raw = jd['template_location_raw']
    
    proceso_principal = jd['proceso_principal']    #CC
    tarea = jd['tarea']                            #CC
    proceso = jd['proceso']                        #CC
    nombre_especifico = jd['nombre_especifico']    #CC
    origen = jd['origen']                          #CC
    tipo_origen = jd['tipo_origen']                #CC
    esquema_origen = jd['esquema_origen']          #CC
    destino = jd['destino']                        #CC
    tipo_destino = jd['tipo_destino']              #CC
    esquema_destino = jd['esquema_destino']        #CC
    fch_reproceso_ini = jd['fch_reproceso_ini']      #CC
    fch_reproceso_fin = jd['fch_reproceso_fin']      #CC
    tipo_extraccion = jd['tipo_extraccion']        #CC
    bloque = jd['bloque']                          #CC
    orden = jd['orden']                            #CC
    tmp_location = jd['temp_location']                            #CC
    tabla_bitacora_tmp = jd['tabla_bitacora_tmp']                            #CC
    
    print("valor tmp", tmp_location)

    # Comando de creacion
    comand_string = 'python3 '+str(code_file_raw)+' --setup_file '+str(setup_file)+' --project '+str(project)+' --save_main_session --runner '+str(runner)+' --region '+str(region)+' --temp_location '+str(temp_location)+' --staging_location '+str(staging_location)+' --template_location '+str(template_location_raw)+' --table_name '+str(table_name)+' --schema '+str(schema)+' --hostname '+str(hostname)+' --port '+str(port)+' --username '+str(username)+' --password "'+str(password)+'" --database '+str(database)+' --sslmode '+str(sslmode)+' --sslrootcert '+str(sslrootcert)+' --sslcert '+str(sslcert)+' --sslkey '+str(sslkey)+ ' --app '+str(app)+' --ssl_certs_db '+str(ssl_certs_db)+' --engine_wrapper '+str(engine_wrapper)+' --proceso_principal '+str(proceso_principal)+' --tarea '+str(tarea)+' --proceso '+str(proceso)+' --nombre_especifico '+str(nombre_especifico)+' --origen '+str(origen)+' --tipo_origen '+str(tipo_origen)+' --esquema_origen '+str(esquema_origen)+' --destino '+str(destino)+' --tipo_destino '+str(tipo_destino)+' --esquema_destino '+str(esquema_destino)+' --fch_reproceso_ini '+str(fch_reproceso_ini)+' --fch_reproceso_fin '+str(fch_reproceso_fin)+' --tipo_extraccion '+str(tipo_extraccion)+' --bloque '+str(bloque)+' --orden '+str(orden)+' --tmp_location '+str(tmp_location)+' --tabla_bitacora_tmp '+str(tabla_bitacora_tmp)
    
    print("comand_string: ", comand_string)
       

# Ejecucion de comando
os.system(comand_string)
print("test hg")

