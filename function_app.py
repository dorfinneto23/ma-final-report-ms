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
from docxtpl import DocxTemplate, RichText #convert to docx 
from markdown2 import markdown # part of organize the text on the conver txt to docx
from bs4 import BeautifulSoup
from docx import Document

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


#  Function filers paragraphs where the disability percentage is not 0% by openai 
def orgainze_content(finalReport):
    
    try:
        mission = mission = (
            f"please organize the following report :\n{finalReport}\n"
        )
        #chat request for content analysis 
        response = client.chat.completions.create(
                    model=openai_model,
                    messages=[
                        {"role": "system", "content": mission},
                        {"role": "user", "content": "Please provide the filtered text without paragraphs where Disability Percentage is 0%."}
                    ]
         )
        logging.info(f"Response from openai: {response.choices[0].message.content}")
        result = response.choices[0].message.content.lower()
        return result
    except Exception as e:
        return f"{str(e)}"  

##HTML Elemnets into docx 
def add_html_to_docx(doc, html_content):
    """Add HTML content to a DOCX document."""
    soup = BeautifulSoup(html_content, "html.parser")
    for element in soup:
        if isinstance(element, str):
            doc.add_paragraph(element)
        elif element.name == "p":
            doc.add_paragraph(element.get_text())
        elif element.name in ["h1", "h2", "h3", "h4", "h5", "h6"]:
            p = doc.add_paragraph(element.get_text())
            p.style = f'Heading{int(element.name[1])}'
        elif element.name == "ul":
            for li in element.find_all("li"):
                doc.add_paragraph(li.get_text(), style='ListBullet')
        elif element.name == "ol":
            for li in element.find_all("li"):
                doc.add_paragraph(li.get_text(), style='ListNumber')
        elif element.name == "strong":
            run = doc.add_paragraph().add_run(element.get_text())
            run.bold = True
        elif element.name == "em":
            run = doc.add_paragraph().add_run(element.get_text())
            run.italic = True
        # Handle other HTML elements as needed

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

def convert_txt_to_docx_with_reference(txt_blob_path, caseid):
    try:
        reference_docx_blob_path = "configuration/custom-reference.docx"
        
        # Download the txt file content
        txt_stream = download_blob_stream(txt_blob_path)
        txt_content = txt_stream.getvalue().decode('utf-8')
        
        # Organize report content using openai
        organize_content = orgainze_content(txt_content)

        # Convert Markdown to HTML
        html_content = markdown(organize_content)

        # Download the reference DOCX template
        reference_stream = download_blob_stream(reference_docx_blob_path)
        reference_file_path = "/tmp/reference.docx"
        with open(reference_file_path, "wb") as ref_file:
            ref_file.write(reference_stream.read())

        # Load the DOCX template
        doc = DocxTemplate(reference_file_path)

        # Prepare context by creating a new DOCX document
        temp_doc_path = "/tmp/temp_doc.docx"
        temp_doc = Document()
        
        # Add HTML content to the temporary DOCX document
        add_html_to_docx(temp_doc, html_content)
        temp_doc.save(temp_doc_path)

        # Replace placeholder content in the template
        template_doc = Document(reference_file_path)
        new_paragraphs = []
        for paragraph in temp_doc.paragraphs:
            new_paragraphs.append(paragraph.text)

        # Insert new paragraphs into the template
        for paragraph in template_doc.paragraphs:
            if '{{ content }}' in paragraph.text:
                # Clear the placeholder paragraph
                p = paragraph._element
                p.getparent().remove(p)
                p._p = p._element = None

                # Add new paragraphs
                for text in new_paragraphs:
                    new_para = template_doc.add_paragraph(text)
                    new_para.style = temp_doc.styles['Normal']

        # Define the output DOCX file path
        output_docx_path = f"/tmp/output_{caseid}.docx"
        
        # Save the DOCX document
        template_doc.save(output_docx_path)

        # Read the output DOCX file back into a stream
        with open(output_docx_path, "rb") as output_file:
            new_doc_stream = io.BytesIO(output_file.read())

        # Save the new DOCX document to Azure Storage
        doc_file_name = "final.docx"
        docx_path = save_final_files(new_doc_stream, caseid, doc_file_name)
        logging.info(f"Document saved to {docx_path}")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")



#Translate conent language 
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
        logging.info(f"An error occurred:, {str(e)}")


# get content csv path from azure table storage 
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
   