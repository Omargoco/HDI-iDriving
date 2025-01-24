import boto3
import os
import json
import zipfile
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor


aws_access_key_id='*******'
aws_secret_access_key='*******'
region_name='us-east-1'
# Configura tu cliente de S3

s3_client = boto3.client('s3',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key,region_name=region_name)

#Constantes
input_bucket_name = 'bkt-idriving-pr'
output_bucket_name = 'bkt-media-idriving-dev'

#Abrir carpetas de raiz
input_folder = "zip/" #"prueba/"
output_folder = 'unzip/'


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

#filepaths = []
#for key in files['Contents']:
#    if key['Key'].endswith('.zip') :
#        filepaths.append(key['Key'])
        
#This will give you list of files in the folder you mentioned as prefix
s3_resource = boto3.resource('s3',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key,region_name=region_name)

#Leer archivo de procesados
print ("leyendo procesados: ")
#aqui me marca error
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
            #print ("descomprimiendo: " + f'{compressed_filepath}')
            compressed_fileinfo = compressed_file.getinfo(compressed_filepath)
            #print ("filepath: " + f'{filepath}')
            #print ("info: " + f'{compressed_fileinfo}')
            #print ("file_size: " + f'{compressed_fileinfo.file_size}')
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

def lambda_handler(event, context):
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps("Fin")
    }

    


