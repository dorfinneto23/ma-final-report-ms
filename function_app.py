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
import markdown2
from bs4 import BeautifulSoup
from docx import Document
import markdown
from docx.shared import Pt, RGBColor
from docx.oxml.ns import qn
from docx.enum.text import WD_PARAGRAPH_ALIGNMENT
from docx.oxml import OxmlElement
from docx.enum.text import WD_ALIGN_PARAGRAPH
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

#-------------------------------------------------------Markdown to DOCX Functions----------------------------------------

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

def parse_html_to_docx(soup, doc):

    def add_paragraph(doc, text, bold=False, italic=False, underline=False, font_size=12, color=None, align_right=False):
        paragraph = doc.add_paragraph()
        run = paragraph.add_run(text)
        run.bold = bold
        run.italic = italic
        run.underline = underline
        run.font.size = Pt(font_size)
        if color:
            run.font.color.rgb = RGBColor(*color)
        if align_right:
            paragraph.alignment = WD_ALIGN_PARAGRAPH.RIGHT
        paragraph.paragraph_format.alignment = WD_PARAGRAPH_ALIGNMENT.RIGHT

    def handle_list(tag, doc, level=0):
        for item in tag.find_all("li", recursive=False):
            p = doc.add_paragraph(style=f'ListBullet{level}' if tag.name == 'ul' else f'ListNumber{level}')
            p.alignment = WD_ALIGN_PARAGRAPH.RIGHT  # Set RTL direction for list items
            for content in item.contents:
                if content.name == 'p':
                    add_paragraph(doc, content.get_text(), align_right=True)
                else:
                    handle_tag(content, p)

            for sublist in item.find_all(['ul', 'ol'], recursive=False):
                handle_list(sublist, doc, level + 1)

    def handle_tag(tag, doc):
        if tag.name == 'h1':
            add_paragraph(doc, tag.get_text(), bold=True, font_size=16, align_right=True)
        elif tag.name == 'p':
            add_paragraph(doc, tag.get_text(), align_right=True)
        elif tag.name in ['ul', 'ol']:
            handle_list(tag, doc)
        elif tag.name == 'em':
            # Handle <em> within other tags
            run = doc.add_paragraph().add_run(tag.get_text())
            run.italic = True
            run.alignment = WD_PARAGRAPH_ALIGNMENT.RIGHT

    # Handle the case where soup.body might be None
    elements = soup.body.children if soup.body else soup.children
    for element in elements:
        handle_tag(element, doc)


def set_docx_rtl(doc):
    # Set the default paragraph style to align right for RTL
    style = doc.styles['Normal']
    paragraph_format = style.paragraph_format
    paragraph_format.alignment = WD_ALIGN_PARAGRAPH.RIGHT

    # Create specific RTL styles for lists
    for i in range(9):
        style_name_bullet = f'ListBullet{i}'
        style_name_number = f'ListNumber{i}'
        if style_name_bullet not in doc.styles:
            style = doc.styles.add_style(style_name_bullet, 1)  # 1 corresponds to a list bullet style
            style.paragraph_format.alignment = WD_ALIGN_PARAGRAPH.RIGHT
        if style_name_number not in doc.styles:
            style = doc.styles.add_style(style_name_number, 1)  # 1 corresponds to a list number style
            style.paragraph_format.alignment = WD_ALIGN_PARAGRAPH.RIGHT

    # Apply RTL to existing paragraphs
    for paragraph in doc.paragraphs:
        paragraph.paragraph_format.alignment = WD_ALIGN_PARAGRAPH.RIGHT

#-------------------------------------------------------Markdown to DOCX Functions----------------------------------------
def convert_txt_to_docx_with_reference(txt_blob_path, caseid,destination_folder):
    try:
        reference_docx_blob_path = "configuration/custom-reference.docx"

        # Download the markdown txt file content
        markdown_txt_stream = download_blob_stream(txt_blob_path)
        markdown_txt_content = markdown_txt_stream.getvalue().decode('utf-8')

        # Debug: Print markdown content
        logging.info(f"Markdown content: {markdown_txt_content}")

        # Convert Markdown content to HTML
        html_content = markdown2.markdown(markdown_txt_content)

        #Debug: Print HTML content
        logging.info(f"HTML content: {html_content}")

        # Parse HTML content
        soup = BeautifulSoup(html_content, "html.parser")
        
        # Adjust HTML for RTL
        for tag in soup.find_all():
            tag['dir'] = 'rtl'
        html_content_rtl = str(soup)

        # Debug: Print adjusted HTML content
        logging.debug(f"RTL adjusted HTML content: {html_content_rtl}")
        
        # Save HTML content to a file
        html_file_name = "final_html.txt"
        save_final_files(html_content_rtl, caseid, html_file_name,destination_folder)
        
        doc = Document()

        # Set document direction to RTL
        set_docx_rtl(doc)

        # Add content to DOCX
        parse_html_to_docx(soup, doc)
        

        # Save the new DOCX document to a stream
        new_doc_stream = io.BytesIO()
        doc.save(new_doc_stream)
        new_doc_stream.seek(0)

        # Save the new DOCX document to Azure Storage
        doc_file_name = "final.docx"
        docx_path = save_final_files(new_doc_stream, caseid, doc_file_name,destination_folder)
        logging.info(f"Document saved to {docx_path}")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")


#-------------------------------------------------------Markdown to DOCX END ----------------------------------------

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
def save_final_files(content,caseid,filename,file_folder):
    try:
        logging.info(f"save_ContentByClinicAreas start, content: {content},caseid: {caseid},filename: {filename}")
        container_name = "medicalanalysis"
        main_folder_name = "cases"
        folder_name="case-"+caseid
        blob_service_client = BlobServiceClient.from_connection_string(connection_string_blob)
        container_client = blob_service_client.get_container_client(container_name)
        basicPath = f"{main_folder_name}/{folder_name}"
        destinationPath = f"{basicPath}/final/{file_folder}/{filename}"
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
    

#main function to create and manage all final filtered asistance response files 
def union_clinic_areas(table_name, caseid):

    destination_folder = "filtered"
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
        if filecontent!="":
            combined_content += "# " + clinic_area + "\n" + filecontent + "\n"
    #save union content of all clinic areas         
    save_final_files(combined_content,caseid,union_file_name,destination_folder)
    text_heb = translate_text(combined_content)
    heb_file_name = f"final-{caseid}-heb.txt"
    #save heb file
    heb_file_path = save_final_files(text_heb,caseid,heb_file_name,destination_folder)
    logging.info(f"union_clinic_areas: combined_content done")
    #convert heb txt file to docx 
    convert_txt_to_docx_with_reference(heb_file_path,caseid,destination_folder)
    
  #main function to create and manage all final filtered asistance response files 
def union_clinic_areas_disabilities_zero(table_name, caseid):

    destination_folder = "disabilities_zero"
    # Create a TableServiceClient object using the connection string
    service_client = TableServiceClient.from_connection_string(conn_str=connection_string_blob)
    
    # Get the table client
    table_client = service_client.get_table_client(table_name=table_name)
    

    # Query the table for entities with the given PartitionKey
    entities = table_client.query_entities(f"PartitionKey eq '{caseid}'")

    # union assistantResponsefiltered into one file for each entity
    combined_content = ""
    union_file_name = f"final-{caseid}-no-disabilities.txt"
    for entity in entities:
        clinic_area = entity['RowKey']
        content_path = entity['assistantResponseNoDisabilities']
        filecontent = get_content(content_path)
        combined_content += "# " + clinic_area + "\n" + filecontent + "\n"
    #save union content of all clinic areas         
    save_final_files(combined_content,caseid,union_file_name,destination_folder)
    text_heb = translate_text(combined_content)
    heb_file_name = f"final-{caseid}-heb-no-disabilities.txt"
    #save heb file
    heb_file_path = save_final_files(text_heb,caseid,heb_file_name,destination_folder)
    logging.info(f"union_clinic_areas: combined_content done")

     

app = func.FunctionApp()

@app.service_bus_queue_trigger(arg_name="azservicebus", queue_name="final-report-process",
                               connection="medicalanalysis_SERVICEBUS") 
def finalReportMs(azservicebus: func.ServiceBusMessage):
    message_data = azservicebus.get_body().decode('utf-8')
    logging.info(f"Received messageesds: {message_data}")
    message_data_dict = json.loads(message_data)
    caseid = message_data_dict['caseid']
    #preparing final files where disabilities is not 0%
    union_clinic_areas_path = union_clinic_areas("contentByClinicAreas",caseid)
    logging.info(f"union_clinic_areas path: {union_clinic_areas_path}")
    #preparing final files where disabilities is  0%
    union_clinic_areas_path_disabilities_zero = union_clinic_areas_disabilities_zero("contentByClinicAreas",caseid)
   