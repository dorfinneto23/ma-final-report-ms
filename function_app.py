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
from docx.oxml.ns import qn
from docx.enum.text import WD_PARAGRAPH_ALIGNMENT
from docx.oxml import OxmlElement
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


def set_docx_rtl(doc):
    """
    Set the document direction to RTL by setting RTL for each paragraph.
    """
    for paragraph in doc.paragraphs:
        set_paragraph_rtl(paragraph)


def set_paragraph_rtl(paragraph):
    """
    Set a paragraph's direction to RTL.
    """
    if paragraph is not None:
        p = paragraph._element
        pPr = p.get_or_add_pPr()
        bidi = OxmlElement('w:bidi')
        bidi.set(qn('w:val'), "1")
        pPr.append(bidi)


def parse_html_to_docx(soup, doc):

    for element in soup.find_all(['h1', 'h2', 'h3', 'p', 'li', 'ol', 'ul']):
        if element.name.startswith('h'):
            # Process headings
            add_heading(doc, element)
        elif element.name == 'p':
            # Process paragraphs
            add_paragraph(doc, element)
        elif element.name == 'li':
            # Handle list items within ordered and unordered lists
            if element.find_parent('ol'):
                add_list_item(doc, element, list_type='number')
            elif element.find_parent('ul'):
                add_list_item(doc, element, list_type='bullet')
        elif element.name == 'ol':
            # Process ordered lists
            add_numbered_list(doc, element)
        elif element.name == 'ul':
            # Process unordered lists
            add_bulleted_list(doc, element)

def add_heading(doc, element):
    """
    Add a heading to the document.
    """
    level = int(element.name[1])  # Get the level from h1, h2, etc.
    heading = doc.add_heading(element.get_text(), level=level)
    set_paragraph_rtl(heading)

def add_paragraph(doc, element):
    """
    Add a paragraph to the document.
    """
    paragraph = doc.add_paragraph(element.get_text())
    set_paragraph_rtl(paragraph)

def add_list_item(doc, element, list_type='bullet'):
    """
    Add a list item to the document.
    """
    if list_type == 'number':
        paragraph = doc.add_paragraph(style='List Number')
    else:
        paragraph = doc.add_paragraph(style='List Bullet')
    paragraph.add_run(element.get_text())
    set_paragraph_rtl(paragraph)

def add_numbered_list(doc, ol_element, level=0):
    """
    Add a numbered list to the document.
    """
    for li in ol_element.find_all('li', recursive=False):
        add_list_item(doc, li, list_type='number')
        # Check for nested lists within this list item
        for nested_ol in li.find_all('ol', recursive=False):
            add_numbered_list(doc, nested_ol, level + 1)
        for nested_ul in li.find_all('ul', recursive=False):
            add_bulleted_list(doc, nested_ul, level + 1)

def add_bulleted_list(doc, ul_element, level=0):
    """
    Add a bulleted list to the document.
    """
    for li in ul_element.find_all('li', recursive=False):
        add_list_item(doc, li, list_type='bullet')
        # Check for nested lists within this list item
        for nested_ol in li.find_all('ol', recursive=False):
            add_numbered_list(doc, nested_ol, level + 1)
        for nested_ul in li.find_all('ul', recursive=False):
            add_bulleted_list(doc, nested_ul, level + 1)


def reformat_lists(soup):
    # Iterate over ordered lists
    for ol in soup.find_all('ol'):
        items = ol.find_all('li', recursive=False)
        for item in items:
            nested_ul = item.find('ul')
            if nested_ul:
                item.insert_after(nested_ul)
                item.insert_after(soup.new_tag('br'))
                nested_ul.wrap(item)

            # Replace inline <h1> with new <li> inside the previous <li>
            inline_heading = item.find_next('h1', recursive=False)
            if inline_heading:
                new_li = soup.new_tag('li')
                new_li.append(inline_heading.extract())
                item.append(new_li)

def convert_txt_to_docx_with_reference(txt_blob_path, caseid,destination_folder):
    try:
        reference_docx_blob_path = "configuration/custom-reference.docx"

        # Download the markdown txt file content
        markdown_txt_stream = download_blob_stream(txt_blob_path)
        markdown_txt_content = markdown_txt_stream.getvalue().decode('utf-8')

        # Debug: Print markdown content
        logging.info(f"Markdown content: {markdown_txt_content}")

        # Convert Markdown content to HTML
        html_content = markdown.markdown(markdown_txt_content)

        #Debug: Print HTML content
        logging.info(f"HTML content: {html_content}")

        # Parse HTML content
        soup = BeautifulSoup(html_content, "html.parser")
        
        # Adjust HTML for RTL
        for tag in soup.find_all():
            tag['dir'] = 'rtl'

         # Add IDs to headings and nest lists within list items
        for tag in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
            tag['id'] = tag.get_text().replace(' ', '_').lower()
        
        # Reformat lists
        reformat_lists(soup)

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
   