from azure.storage.blob import ContainerClient, BlobClient
from datetime import datetime
from elasticsearch import Elasticsearch
import hashlib
from io import StringIO
import json
import langchain.document_loaders
from langchain_community.vectorstores.elasticsearch import ElasticsearchStore
from langchain_openai.embeddings import OpenAIEmbeddings, AzureOpenAIEmbeddings
from langchain.schema import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter
import logging
import os
import pandas as pd
from pathlib import Path
import tempfile
import time

# Constants - now reading directly from environment variables
ELASTIC_CLOUD_ID = os.environ['ELASTIC_CLOUD_ID']
ELASTIC_USERNAME = os.environ['ELASTIC_USERNAME']
ELASTIC_PASSWORD = os.environ['ELASTIC_PASSWORD']

OPEN_AI_KEY = os.environ['OPEN_AI_KEY']
OPEN_AI_DEPLOYMENT = os.environ['OPEN_AI_DEPLOYMENT']
OPEN_AI_MODEL = os.environ['OPEN_AI_MODEL']
OPEN_AI_BASE = os.environ['OPEN_AI_BASE']
OPEN_AI_TYPE = os.environ['OPEN_AI_TYPE']
OPEN_AI_VERSION = os.environ['OPEN_AI_VERSION']

AZURE_CONNECTION_STRING = os.environ['askmaddiknowledgeset_STORAGE'] # Using the connection string from function app
DIRECTORY_CONNECTION_STRING = os.environ.get('DIRECTORY_CONNECTION_STRING', '')  # Optional for directory-based loading


PRODUCT_NAME='PRODUCT_NAME'
PREFIX = 'PREFIX'
#other constants
SOURCE = 'source'
PAGE = 'page'
DOC_TYPE = 'doc_type'
MD5HEXHASH = 'md5HexHash'
FILE_NAME = 'file_name'
APPLICATION = 'application'
HITS = 'hits'
ACCESS_LEVEL = 'access_level' #Internal Or External Access for a document

DOCUMENTATION = 'documentation'
TEAMS = 'teams'
SALESFORCE = 'salesforce'

PRODUCT_AREAS = ['ATO', 'BillBlast', 'Docketing', 'Expert', 'Handshake', 'iTimekeep', 'Rainmaker', 'vibyaderant', 'testing']
PRODUCT_CONTAINERS = {
    'ATO': 'ato',
    'BillBlast': 'billblast',
    'Docketing': 'docketing', 
    'Expert': 'expert-v1',
    'Handshake': 'handshake',
    'iTimekeep': 'itimekeep',
    'Rainmaker': 'rainmaker', 
    'vibyaderant': 'vibyaderant',
    'testing': 'testing'}
PRODUCT_INDEXES = {
    'ATO': ['ato'],
    'BillBlast': ['billblast'],
    'Docketing': ['docketing'], 
    'Expert': ['expert-v1', 'expert-prod'], 
    'Handshake': ['handshake'],
    'iTimekeep': ['itimekeep'],
    'Rainmaker': ['rainmaker'],
    'vibyaderant': ['vibyaderant'],
    'testing': ['testing']}





def split_sample_questions_by_line(split_documents:list[Document]):
    logging.log(logging.DEBUG, 'split_sample_questions_by_line')
    split_by_line_documents: list[Document]=[]
    for doc in split_documents:
        question_number = 1
        for line in StringIO(doc.page_content).readlines():
            if line.strip():
                temp_metadata = {}
                temp_metadata[SOURCE] = doc.metadata.get(SOURCE)
                #set page to question_number this helps with performance when it sets ids below since source+page will be unique for all questions
                temp_metadata[PAGE] = question_number
                temp_metadata[DOC_TYPE] = "sample_questions"
                split_by_line_documents.append(Document(page_content=line, metadata=temp_metadata))
                question_number += 1
    return split_by_line_documents





def load_teams_json(record: dict, metadata:dict):
    teams_metadata_include = set(['TeamId', 'ChannelId', 'MessageId', 'Date', 'Url'])
    metadata['doc_type'] = 'teams'
    metadata.update({k: v for k, v in record.items() if k in teams_metadata_include})
    text = record['Conversation']
    return text, metadata




def load_salesforce_json(record: dict, metadata:dict):
    meta_include = set(["TITLE", "URLNAME"])
    text_exclude = set(["AUTHOR__C", "CREATEDBYID", "ID", "KNOWLEDGEARTICLEID", "ARTICLENUMBER", "PUBLISHSTATUS", "RECORDTYPEID", "VERSIONNUMBER", "OWNERID" ])
    metadata['doc_type'] = 'salesforce'
    metadata.update({key: val for key, val in record.items() if (key in text_exclude) or (key in meta_include)})
    text = "\n\n".join(["{}: {}".format(key, val) for key, val in record.items() if (isinstance(val, str)) and (key not in text_exclude)])
    return text, metadata




def load_json_document(json_file_path:str, json_loader_func) -> list[Document]:
    logging.log(logging.DEBUG, 'load_json_document')
    documents: list[Document] = []
    with open(json_file_path, 'r', encoding='utf-8') as f:
        record = json.load(f)
        metadata = {}
        text, metadata = json_loader_func(record, metadata)
        metadata[SOURCE] = json_file_path
        documents.append(Document(page_content=text, metadata=metadata))
    return documents




def langchain_load_document(full_file_name:str):
    logging.log(logging.DEBUG, 'langchain_load_document for file: ' + full_file_name)
    loaded_document = []
    split_name = os.path.splitext(full_file_name)
    file_extension_lower = split_name[1].lower()
    # if file_extension_lower == '.pdf':
    #     document_loader = langchain.document_loaders.PyPDFLoader(full_file_name)
    if file_extension_lower == '.json':
        if split_name[0].endswith(TEAMS):
            json_load_func = load_teams_json
        elif split_name[0].endswith(SALESFORCE):
            json_load_func = load_salesforce_json
        return load_json_document(full_file_name, json_load_func)
    elif file_extension_lower == '.xlsx' and "Resolved Issues in Updates".casefold() in full_file_name.casefold(): 
        excel_documents: list[Document] = []
        df = pd.read_excel(full_file_name, sheet_name=None, dtype=str)
        pd.options.display.max_colwidth = 1000
        for frame in df:
            df[frame]=df[frame].fillna('')
            text = ''
            for _, row in df[frame].iterrows():
                text = text + row.to_string().replace("  ", "")+ '\n'
            metadata = {}
            metadata['source'] = full_file_name
            excel_documents.append(Document(page_content=text, metadata=metadata))       
        return excel_documents
    else:
        document_loader = langchain.document_loaders.UnstructuredFileLoader(full_file_name)
    try:
        loaded_document = document_loader.load()
    except Exception as e:
        print("Unable to load file: " + full_file_name + 'with error: ' + str(e))
        logging.exception("Unable to load file: " + full_file_name)
    
    return loaded_document





def langchain_split_documents(loaded_documents: list[Document], is_sample_questions:bool):
    logging.log(logging.DEBUG, 'langchain_split_documents')
    if is_sample_questions:
        split_documents = split_sample_questions_by_line(loaded_documents)
    else:
        chunk_size = 1500
        chunk_overlap = 150
        text_splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
        split_documents = text_splitter.split_documents(loaded_documents)
    return split_documents





#may need to be running as admin to access network location
def load_from_directory(is_sample_questions:bool):
    path = os.getenv(DIRECTORY_CONNECTION_STRING)
    logging.log(logging.DEBUG, 'load_from_directory')   
    path = Path(path)
    #gets all non hidden files(**/[!,]*) in given path, for entire subtree of path, return full path of file
    files_to_process = [str(file) for file in path.glob('**/[!.]*') if file.is_file()]
    documents: list[Document] = []
    for file_name in files_to_process:
        document = langchain_load_document(file_name)
        if len(document) > 0:
            documents.extend(document)
    return langchain_split_documents(documents, is_sample_questions)





def load_from_azure_container(container_name:str, is_sample_questions:bool, prefix:str, last_processed_time:datetime):
    if is_sample_questions:
        connection_string = AZURE_CONNECTION_STRING
        prefix = None
    else:
        connection_string = AZURE_CONNECTION_STRING
    logging.info(f'Loading from Azure container: {container_name}')
    azure_container = ContainerClient.from_connection_string(conn_str=connection_string, container_name=container_name)
    #set prefix if you only want to grab specific items or folders in container (i.e. Billing of Billing/Teams)
    blob_list = azure_container.list_blobs(name_starts_with=prefix)
    documents: list[Document] = []
    for blob in blob_list:
        #filter on blob.last_modified and if it isn't newer than the last time it was processed skip it
        if blob.last_modified.timestamp() < last_processed_time.timestamp():
            continue
        client = BlobClient.from_connection_string(conn_str=connection_string, container_name=container_name, blob_name=blob.name)
        with tempfile.TemporaryDirectory() as temp_dir:
            full_file_path = f"{temp_dir}/{container_name}/{blob.name}"
            os.makedirs(os.path.dirname(full_file_path), exist_ok=True)
            with open(f"{full_file_path}", "wb") as file:
                blob_data = client.download_blob()
                blob_data.readinto(file)
            document = langchain_load_document(full_file_path)
            print(full_file_path)
            logging.log(logging.INFO, f'Loaded document: {full_file_path}')
            documents.extend(document)
    return langchain_split_documents(documents, is_sample_questions)





def load_documents(from_azure_container:bool, from_directory:bool, is_sample_questions:bool, container_name:str, prefix:str, last_processed_time:datetime):
    if from_azure_container:
        split_documents = load_from_azure_container(container_name, is_sample_questions, prefix, last_processed_time)
    elif from_directory:
        split_documents = load_from_directory(is_sample_questions)
    else:
        raise Exception("Unknown source to load documents from")
    return split_documents





def update_metadata(split_documents: list[Document], container_name:str, from_azure_container:bool):
    ids = []
    texts = [doc.page_content for doc in split_documents]
    metadata = [doc.metadata for doc in split_documents]

    for doc in split_documents:       
        docId = os.path.basename(doc.metadata.get(SOURCE)) + '.' + str(doc.metadata.get(PAGE))
        newDocId = docId
        count = 0
        while newDocId in ids:
            count = count + 1
            newDocId = docId + '.' + str(count)
        ids.append(newDocId)

    for t, m in zip(texts, metadata):
        md5Hash = hashlib.md5(t.encode()).hexdigest()
        page_number = -1
        if m.get(PAGE) is not None:
            page_number = m[PAGE]
            #PyPDFLoader page numbers start at 0, so increment by 1
            if ".pdf" in m[SOURCE]:
                page_number = page_number + 1
        #only configured to detect application from azure container
        split_source = m[SOURCE].split(container_name + '/', 2)
        access_level = split_source[1].split('/', 2)[0]
        if from_azure_container:
            application_name = split_source[1].split('/', 3)[1]
            #also update source to be container_name/blob_name to make it easier to build download link
            m[SOURCE] = [{FILE_NAME:container_name + '/' + split_source[1], PAGE:page_number, APPLICATION: application_name}]
        else:
            #otherwise use product_name (which is same as container_name and elastic_index_name) from config
            application_name = container_name
            m[SOURCE] = [{FILE_NAME:m[SOURCE], PAGE:page_number, APPLICATION: application_name}]
        m[MD5HEXHASH] = md5Hash
        if m.get(DOC_TYPE) is None:
            m[DOC_TYPE] = DOCUMENTATION
        if m.get(ACCESS_LEVEL) is None:
            m[ACCESS_LEVEL] = access_level
        #remove page as that information is now in source and having them just adds noise
        if m.get(PAGE) is not None:
            del m[PAGE]
    return texts, metadata, ids





def check_for_duplicates(texts, metadata, ids):    
    #checks selected documents for any chunks that have the same hash and updates the metadata to show both files and removes the second instance of them
    #if you rerun this without resetting/rerunning previous code the source value will get messed up
    hashes = []
    indexesToRemove = []
    for i, text in enumerate(texts):
        md5Hash = hashlib.md5(text.encode()).hexdigest()
        if md5Hash in hashes:
            matchFound = False
            for j, metadatum in enumerate(metadata):
                if metadatum.get(MD5HEXHASH) == md5Hash:
                    indexesToRemove.append(i)
                    matchFound = True
                    break
            if matchFound:
                metadata[j][SOURCE].extend(metadata[i][SOURCE])
        else:
            hashes.append(md5Hash)
            metadata[i][MD5HEXHASH] = md5Hash
            
    indexesToRemove.reverse()
    print("Removing duplicates: " + str(len(indexesToRemove)))
    for index in indexesToRemove:
        del texts[index]
        del metadata[index]
        del ids[index]
    



def create_vector_store(index_name:str):
    logging.info(f'Creating vector store for index: {index_name}')
    
    # Use environment variables directly
    if OPEN_AI_TYPE == 'azure':
        embedding = AzureOpenAIEmbeddings(
            openai_api_key=OPEN_AI_KEY,
            deployment=OPEN_AI_DEPLOYMENT, 
            model=OPEN_AI_MODEL, 
            azure_endpoint=OPEN_AI_BASE,
            openai_api_type=OPEN_AI_TYPE, 
            openai_api_version=OPEN_AI_VERSION
        )
    else:
        embedding = OpenAIEmbeddings(
            openai_api_key=OPEN_AI_KEY,
            deployment=OPEN_AI_DEPLOYMENT, 
            model=OPEN_AI_MODEL, 
            openai_api_base=OPEN_AI_BASE,
            openai_api_type=OPEN_AI_TYPE, 
            openai_api_version=OPEN_AI_VERSION
        )

    es_connection = Elasticsearch(
        cloud_id=ELASTIC_CLOUD_ID,
        basic_auth=[ELASTIC_USERNAME, ELASTIC_PASSWORD]
    )

    return ElasticsearchStore(
        index_name=index_name,
        es_connection=es_connection,
        embedding=embedding
    )



#sets the source value for elastic, checks to make sure the new file isn't already listed as a source item
def compute_new_source_value(existing_source, new_file_source):
    need_to_run_update = False
    if type(existing_source) is list and type(new_file_source) is list:
        for file_source in new_file_source:
            if file_source not in existing_source:
                existing_source = existing_source.append(file_source)
                need_to_run_update = True
    else:
        logging.log(logging.ERROR, 'Either existing or new source field not a list')
        raise ValueError('Either existing or new source field not a list')
    return need_to_run_update, existing_source




def update_source_in_elastic(vectorElastic, elastic_id, elastic_index_name, existing_source, new_file_source):
    need_to_run_update, existing_source = compute_new_source_value(existing_source, new_file_source)
    if need_to_run_update:
        update_script = {'source':"ctx._source.metadata.source = params.source", 'lang':'painless', 'params':{'source': existing_source}}
        vectorElastic.client.update(index=elastic_index_name, id=elastic_id, script=update_script)



def check_elastic_for_duplicates(vectorElastic: ElasticsearchStore, elastic_index_name:str, metadata:list, texts:list, ids:list):
    logging.log(logging.DEBUG, 'check_elastic_for_duplicates')
    if check_elastic_for_duplicates:
        indexes_in_elastic = []
        for i, data in enumerate(metadata):
            index_exists = vectorElastic.client.indices.exists(index=elastic_index_name)
            if index_exists:
                hexHash = data.get(MD5HEXHASH)
                textExists = vectorElastic.client.search(index=elastic_index_name, q="metadata.md5HexHash: " + hexHash)
                if len(textExists.body[HITS][HITS]) == 1:
                    existing_id_with_hash = textExists.body[HITS][HITS][0]["_id"]
                    if existing_id_with_hash != ids[i]:
                        existing_source = textExists.body[HITS][HITS][0]['_source']['metadata'][SOURCE]  
                        update_source_in_elastic(vectorElastic, existing_id_with_hash, elastic_index_name, existing_source, data.get(SOURCE))                
                        indexes_in_elastic.append(i)
                    
        #now remove those whose hash already exists in elastic
        indexes_in_elastic.reverse()
        for elastic_index in indexes_in_elastic:
            del texts[elastic_index]
            del metadata[elastic_index]
            del ids[elastic_index]




def upload_to_elastic(vectorElastic:ElasticsearchStore, texts:list, metadata:list, ids:list):
    #texts, metadata, and ids should all be the same length
    #Azure currently accepts a max of 16 at a time so split them in to lists of 16 items 
    #(Too many inputs. The max number of inputs is 16.  We hope to increase the number of inputs per request soon. Please contact us through an Azure)
    chunk_range = 8
    #if timeout or exceed call limit can update range start to be count * chunk_range
    chunked_texts = [texts[x:x+chunk_range] for x in range(chunk_range*0, len(texts), chunk_range)]
    chunked_metadata = [metadata[x:x+chunk_range] for x in range(chunk_range*0, len(metadata), chunk_range)]
    chunked_ids = [ids[x:x+chunk_range] for x in range(chunk_range*0, len(ids), chunk_range)]
    #only add_texts allows us to specify metadata and ids
    count = 0
    for t, m, i in zip(chunked_texts, chunked_metadata, chunked_ids):
        vectorElastic.add_texts(
            texts = t,
            metadatas = m,
            ids = i
        )    
        count = count + 1
        print(count)
        #time.sleep(1)



def run_upload_to_elastic(from_azure_container: bool, from_directory: bool, is_sample_questions: bool, check_for_duplicates_in_elastic: bool, last_processed_time: datetime, container_name: str, prefix: str = None):
    """Modified to process only a specific container"""
    logging.info(f'Processing container: {container_name} | from_azure_container: {from_azure_container} | from_directory: {from_directory} | is_sample_questions: {is_sample_questions} | check_elastic_for_duplicates: {check_for_duplicates_in_elastic}')
    
    # Find the product area for this container
    product_area = next((area for area, container in PRODUCT_CONTAINERS.items() if container == container_name), None)
    if not product_area:
        raise ValueError(f"No product area found for container: {container_name}")
    
    # Only process the specific container's indexes
    indexes = PRODUCT_INDEXES.get(product_area, [])
    if not indexes:
        raise ValueError(f"No indexes found for product area: {product_area}")
    
    print(f'Product Area: {product_area}')
    print(f'Container Name: {container_name}')
    
    # Loop over just the specific container's indexes
    for elastic_index_name in indexes:
        print(f'Elastic Index Name: {elastic_index_name}')
        split_documents = load_documents(from_azure_container, from_directory, is_sample_questions, container_name, prefix, last_processed_time)
        texts, metadata, ids = update_metadata(split_documents, container_name, from_azure_container)
        
        if len(texts) == 0:
            logging.info(f'No documents to upload for container: {container_name}, index: {elastic_index_name}')
            continue
        
        print(f'Number of documents to upload: {len(texts)}')
        check_for_duplicates(texts, metadata, ids)
        print(f'Number of documents after checking for duplicates: {len(texts)}')
        vectorElastic = create_vector_store(elastic_index_name)
        print(vectorElastic.index_name)
        
        if check_for_duplicates_in_elastic:
            check_elastic_for_duplicates(vectorElastic, elastic_index_name, metadata, texts, ids)
        
        uploading_message = f'Uploading {len(texts)} chunks, from {container_name} to {elastic_index_name}'
        print(uploading_message)
        logging.info(uploading_message)
        upload_to_elastic(vectorElastic, texts, metadata, ids)

