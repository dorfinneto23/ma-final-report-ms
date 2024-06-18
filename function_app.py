import azure.functions as func
import logging
import os #in order to get parameters values from azure function app enviroment vartiable - sql password for example 
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient # in order to use azure container storage
import io # in order to download pdf to memory and write into memory without disk permission needed 
import json # in order to use json 
import pyodbc #for sql connections 
from azure.servicebus import ServiceBusClient, ServiceBusMessage # in order to use azure service bus 
from openai import AzureOpenAI #for using openai services 
from azure.data.tables import TableServiceClient, TableClient, UpdateMode # in order to use azure storage table  
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError # in order to use azure storage table  exceptions 
import csv #helping convert json to csv
import requests #in order to use translation function 
import uuid  #in order to use translation function 

# Azure Blob Storage connection string
connection_string_blob = os.environ.get('BlobStorageConnString')

#Azure service bus connection string 
connection_string_servicebus = os.environ.get('servicebusConnectionString')

#translate key
translate_key = os.environ.get('translate_key')

# Define connection details
server = 'medicalanalysis-sqlserver.database.windows.net'
database = 'medicalanalysis'
username = os.environ.get('sql_username')
password = os.environ.get('sql_password')
driver= '{ODBC Driver 18 for SQL Server}'


#Translate conent language 
def translate_text(text, to_language='he'):
    key = translate_key
    endpoint = "https://api.cognitive.microsofttranslator.com/"
    location = "eastus"
    path = '/translate'
    constructed_url = endpoint + path

    params = {
        'api-version': '3.0',
        'from': 'en',
        'to': [to_language]
    }

    headers = {
        'Ocp-Apim-Subscription-Key': key,
        'Ocp-Apim-Subscription-Region': location,
        'Content-type': 'application/json',
        'X-ClientTraceId': str(uuid.uuid4())
    }

    body = [{
        'text': text
    }]

    response = requests.post(constructed_url, params=params, headers=headers, json=body)
    response.raise_for_status()

    translations = response.json()
    translated_text = translations[0]['translations'][0]['text']
    return translated_text

#save ContentByClinicAreas content 
def save_final_files(content,caseid,filename):
    try:
        logging.info(f"save_ContentByClinicAreas start, content: {content},caseid: {caseid},filename: {filename}")
        container_name = "medicalanalysis"
        main_folder_name = "cases"
        folder_name="case-"+caseid
        blob_service_client = BlobServiceClient.from_connection_string(connection_string_blob)
        container_client = blob_service_client.get_container_client(container_name)
        basicPath = f"{main_folder_name}/{folder_name}"
        destinationPath = f"{basicPath}/final/{filename}"
        # Upload the blob and overwrite if it already exists
        blob_client = container_client.upload_blob(name=destinationPath, data=content, overwrite=True)
        logging.info(f"the ContentByClinicAreas content file url is: {blob_client.url}")
        return destinationPath
    
    except Exception as e:
        print("An error occurred:", str(e))

# get content csv path from azure table storage 
def get_contentcsv(path):
    try:
        logging.info(f"get_contentcsv function strating, path value: {path}")
        container_name = "medicalanalysis"
        blob_service_client = BlobServiceClient.from_connection_string(connection_string_blob)
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(path)
        download_stream = blob_client.download_blob()
        filecontent  = download_stream.read().decode('utf-8')
        logging.info(f"get_contentcsv: data from the txt file is {filecontent}")
        return filecontent
    except Exception as e:
        logging.error(f"get_contentcsv: Error update case: {str(e)}")
        return None    
    

#this function union all clinic areas content into one file 
def union_clinic_areas(table_name, caseid):
    # Create a TableServiceClient object using the connection string
    service_client = TableServiceClient.from_connection_string(conn_str=connection_string_blob)
    
    # Get the table client
    table_client = service_client.get_table_client(table_name=table_name)
    

    # Query the table for entities with the given PartitionKey
    entities = table_client.query_entities(f"PartitionKey eq '{caseid}'")

    # union assistantResponsefiltered into one file for each entity
    combined_content = ""
    union_file_name = f"final-{caseid}.txt"
    for entity in entities:
        clinic_area = entity['RowKey']
        content_path = entity['assistantResponsefiltered']
        filecontent = get_contentcsv(content_path)
        if filecontent!="no disabilities found.":
            combined_content += "# " + clinic_area + "\n" + filecontent + "\n"
    #save union content of all clinic areas         
    save_final_files(combined_content,caseid,union_file_name)
    text_heb = translate_text(combined_content)
    heb_file_name = f"final-{caseid}-heb.txt"
    save_final_files(text_heb,caseid,heb_file_name)
    logging.info(f"union_clinic_areas: combined_content done")
    


app = func.FunctionApp()

@app.service_bus_queue_trigger(arg_name="azservicebus", queue_name="final-report-process",
                               connection="medicalanalysis_SERVICEBUS") 
def finalReportMs(azservicebus: func.ServiceBusMessage):
    message_data = azservicebus.get_body().decode('utf-8')
    logging.info(f"Received messageesds: {message_data}")
    message_data_dict = json.loads(message_data)
    caseid = message_data_dict['caseid']
    union_clinic_areas_path = union_clinic_areas("contentByClinicAreas",caseid)
    logging.info(f"union_clinic_areas path: {union_clinic_areas_path}")