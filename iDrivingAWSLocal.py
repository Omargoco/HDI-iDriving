import json
import boto3
import zipfile
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor

#Constantes
input_bucket_name = 'bkt-idriving-pr'
output_bucket_name = 'bkt-media-idriving-dev'

#Abrir carpetas de raiz
input_folder = "zip/" #"prueba/"
output_folder = 'unzip/'

s3_client = boto3.client('s3', use_ssl=False)

#funcion solo permite 1000 registros y se cambia el paginado
#files =  s3_client.list_objects_v2(Bucket=input_bucket_name, Prefix=input_folder, Delimiter = "/")
#######################################
# Create a reusable Paginator
paginator = s3_client.get_paginator('list_objects_v2')

# Create a PageIterator from the Paginator
page_iterator = paginator.paginate(Bucket=input_bucket_name)

########################################

filepaths = []
for files in page_iterator:
    for key in files['Contents']:
        if key['Key'].endswith('.zip') :
            filepaths.append(key['Key'])

filepaths.sort() 

#This will give you list of files in the folder you mentioned as prefix
s3_resource = boto3.resource('s3')

#Leer archivo de procesados
print ("leyendo procesados: ")
obj_procesados = s3_resource.meta.client.get_object(
    Bucket=output_bucket_name,
    Key= output_folder + "procesados.list")
string_procesados = obj_procesados['Body'].read().decode("utf-8")

for filepath in filepaths:
    filename = filepath.split("/")[-1]
    print ("archivo actual: " + filename)
    procesado = filename in string_procesados.split(",")
    es_zip = filename.endswith('.zip')
    print ("procesado: " + "true" if procesado else "false")
    
    if not procesado and es_zip:
        
        zip_obj = s3_resource.Object(bucket_name=input_bucket_name, key=filepath)
        buffer = BytesIO(zip_obj.get()["Body"].read())
        compressed_file = zipfile.ZipFile(buffer)
        
        for compressed_filepath in compressed_file.namelist():            
            compressed_fileinfo = compressed_file.getinfo(compressed_filepath)
            current_folder = compressed_filepath.split("/")[-2]
            print ("current_folder:" + current_folder)
            
            if current_folder != "taglogs" :
                if compressed_fileinfo.file_size == 0 :
                    print ("folder: " + output_folder + compressed_filepath)
                    s3_client.put_object(Bucket=output_bucket_name, Key=(output_folder + compressed_filepath))
                    
                if compressed_fileinfo.file_size > 0 :
                    print ("archivo: " + output_folder + compressed_filepath)

                    print ("creando archivo: " + output_folder + compressed_filepath)
                    s3_resource.meta.client.upload_fileobj(
                        compressed_file.open(compressed_filepath),
                        Bucket=output_bucket_name,
                        Key= output_folder + compressed_filepath)
    
        buffer.close()
        string_procesados = string_procesados + filename + ","
        print ("procesados:" + string_procesados)

        #Actualizar archivo de procesados
        print ("actualizando procesados: " + string_procesados)
        s3_resource.meta.client.put_object(
            Body=string_procesados,
            Bucket=output_bucket_name,
            Key= output_folder + "procesados.list")
#string_procesados = ""
