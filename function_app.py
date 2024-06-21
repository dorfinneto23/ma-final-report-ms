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
import requests #in order to use translation function 
import uuid  #in order to use translation function 
from markdown2 import markdown # part of organize the text on the conver txt to docx
from bs4 import BeautifulSoup
from docx import Document
import markdown
from docx.shared import Pt
from docx.enum.text import WD_PARAGRAPH_ALIGNMENT
import tempfile


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


#OpenAI Details 
client = AzureOpenAI(
  api_key = os.environ.get('AzureOpenAI_pi_key'),
  api_version = "2024-02-01",
  azure_endpoint = "https://openaisponsorship.openai.azure.com/"
)

openai_model = "ProofitGPT4o"


#  Function using openai to organize the text  
def orgainze_content(finalReport):
    
    try:
        mission = mission = (
            f"please add markdown and organize the following report in Hebrew :\n{finalReport}\n"
        )
        #chat request for content analysis 
        response = client.chat.completions.create(
                    model=openai_model,
                    messages=[
                        {"role": "system", "content": mission},
                        {"role": "user", "content": "please add markdown and organize the following report in Hebrew"}
                    ]
         )
        logging.info(f"Response from openai: {response.choices[0].message.content}")
        result = response.choices[0].message.content.lower()
        return result
    except Exception as e:
        return f"{str(e)}"  


# Helper function to download blob content to stream 
def download_blob_stream(path):
        # Create a BlobServiceClient using the connection string
        container_name = "medicalanalysis"
        blob_service_client = BlobServiceClient.from_connection_string(connection_string_blob)
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(path)
        stream = io.BytesIO()
        blob_client.download_blob().download_to_stream(stream)
        stream.seek(0)
        return stream
# Replace placeholder with content in the DOCX document.
def replace_placeholder_with_content(doc, placeholder, html_content):
    for paragraph in doc.paragraphs:
        if placeholder in paragraph.text:
            # Clear the paragraph
            paragraph.clear()
            # Parse HTML content
            soup = BeautifulSoup(html_content, "html.parser")
            for element in soup.descendants:
                if element.name is None:  # It's a NavigableString
                    paragraph.add_run(element.strip())
                elif element.name == 'strong':
                    paragraph.add_run(element.text).bold = True
                elif element.name == 'em':
                    paragraph.add_run(element.text).italic = True
                elif element.name == 'h1':
                    run = paragraph.add_run(element.text)
                    run.font.size = Pt(24)
                    paragraph.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
                elif element.name == 'h2':
                    run = paragraph.add_run(element.text)
                    run.font.size = Pt(18)
                    paragraph.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
                elif element.name == 'p':
                    paragraph.add_run(element.text)
                elif element.name == 'br':
                    paragraph.add_run().add_break()
            return

def convert_txt_to_docx_with_reference(txt_blob_path, caseid):
    try:
        reference_docx_blob_path = "configuration/custom-reference.docx"

        # Download the markdown txt file content
        markdown_txt_stream = download_blob_stream(txt_blob_path)
        markdown_txt_content = markdown_txt_stream.getvalue().decode('utf-8')

        # Download the reference DOCX template
        reference_stream = download_blob_stream(reference_docx_blob_path)
        with tempfile.NamedTemporaryFile(delete=False, suffix='.docx') as tmp_template:
            tmp_template.write(reference_stream.read())
            reference_file_path = tmp_template.name

        # Convert Markdown content to HTML
        html_content = markdown.markdown(markdown_txt_content)

        # Load DOCX template
        doc = Document(reference_file_path)

        # Replace the placeholder with converted HTML content
        replace_placeholder_with_content(doc, '{{ content }}', html_content)

        # Define the output DOCX file path
        with tempfile.NamedTemporaryFile(delete=False, suffix='.docx') as tmp_output:
            output_docx_path = tmp_output.name
            doc.save(output_docx_path)

        # Read the output DOCX file back into a stream
        with open(output_docx_path, "rb") as output_file:
            new_doc_stream = io.BytesIO(output_file.read())

        # Save the new DOCX document to Azure Storage
        doc_file_name = "final.docx"
        docx_path = save_final_files(new_doc_stream, caseid, doc_file_name)
        logging.info(f"Document saved to {docx_path}")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")



#Translate content given language 
def translate_text(text, to_language='he'):
    try:
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
    except Exception as e:
        logging.error(f"An error occurred:, {str(e)}")

#save files in "final" folder
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
        logging.info(f"An error occurred:, {str(e)}")


# get content from txt file by path from azure table storage 
def get_content(path):
    try:
        logging.info(f"get_content function strating, path value: {path}")
        container_name = "medicalanalysis"
        blob_service_client = BlobServiceClient.from_connection_string(connection_string_blob)
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(path)
        download_stream = blob_client.download_blob()
        filecontent  = download_stream.read().decode('utf-8')
        logging.info(f"get_content: data from the txt file is {filecontent}")
        return filecontent
    except Exception as e:
        logging.error(f"get_content: Error update case: {str(e)}")
        return None    
    

#main function to create and manage all final files 
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
        filecontent = get_content(content_path)
        if filecontent!="no disabilities found.":
            combined_content += "# " + clinic_area + "\n" + filecontent + "\n"
    #save union content of all clinic areas         
    save_final_files(combined_content,caseid,union_file_name)
    text_heb = translate_text(combined_content)
    heb_file_name = f"final-{caseid}-heb.txt"
    #save heb file
    heb_file_path = save_final_files(text_heb,caseid,heb_file_name)
    logging.info(f"union_clinic_areas: combined_content done")
    #convert heb txt file to docx 
    convert_txt_to_docx_with_reference(heb_file_path,caseid)
    
   

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
   