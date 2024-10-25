import os
import logging
from datetime import datetime
from elasticsearch import Elasticsearch
from azure.storage.blob import ContainerClient
from datetime import timezone

ELASTIC_CLOUD_ID = os.environ['ELASTIC_CLOUD_ID']
ELASTIC_USERNAME = os.environ['ELASTIC_USERNAME']
ELASTIC_PASSWORD = os.environ['ELASTIC_PASSWORD']
AZURE_CONNECTION_STRING = os.environ['askmaddiknowledgeset_STORAGE']

PRODUCT_AREAS = ['ATO', 'BillBlast', 'Docketing', 'Expert', 'Handshake', 'iTimekeep', 'Rainmaker', 'vibyaderant']
PRODUCT_INDEXES = {
    'ATO': ['ato'],
    'BillBlast': ['billblast'],
    'Docketing': ['docketing'], 
    'Expert': ['expert-v1', 'expert-prod'], 
    'Handshake': ['handshake'],
    'iTimekeep': ['itimekeep'],
    'Rainmaker': ['rainmaker'],
    'vibyaderant': ['vibyaderant']}

def ensure_timezone_aware(dt: datetime) -> datetime:
    """
    Ensures a datetime object is timezone-aware by adding UTC timezone if naive
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt

def delete_from_elastic_by_source_filename(file_name_to_delete:str, index_name:str, elastic_client:Elasticsearch):
    logging.log(logging.INFO, f'Running delete by query on index {index_name} for metadata.source.file_name {file_name_to_delete}')
    delete_query = {
        "query_string": {
            "default_field": "metadata.source.file_name", 
            "query": f"\"{file_name_to_delete}\"",
            "default_operator": "AND"
        }
    }
    logging.log(logging.INFO, f'Delete query: {delete_query}')
    elastic_client.delete_by_query(index=index_name, query=delete_query)

def update_or_remove_from_elastic(search_text:str, index_name:str, elastic_client:Elasticsearch):
    search_size=100
    logging.log(logging.INFO, f'Searching elastic index {index_name} for metadata.source.file_name {search_text}')
    search_response = elastic_client.search(index=index_name, size=search_size, query={
        "query_string": { "default_field": "metadata.source.file_name", "query": f"\"{search_text}\"", "default_operator": "AND" }
    })
    search_results = search_response.body['hits']['hits']
    for result in search_results:
        existing_id_with_hash = result["_id"]
        existing_source = result['_source']['metadata']['source']
        for source in existing_source:
            if search_text in source:
                existing_source.remove(source)
        if len(existing_source) <= 1:
            logging.log(logging.INFO, f'Deleting chunk with id: {existing_id_with_hash}')
            elastic_client.delete(index=index_name, id=existing_id_with_hash)
        else:
            update_script = {'source':"ctx._source.metadata.source = params.source", 'lang':'painless', 'params':{'source': existing_source}}
            elastic_client.update(index=index_name, id=existing_id_with_hash, script=update_script)
    if len(search_results) == search_size:
        update_or_remove_from_elastic(search_text, index_name, elastic_client)

def delete_by_search_text(search_text:str, index_name:str, es_connection:Elasticsearch, hard_delete:bool):
    if search_text is None or search_text == '':
        search_text_empty_message = 'search_text cannot be empty'
        print(search_text_empty_message)
        logging.log(logging.ERROR, search_text_empty_message)
        return            
    if hard_delete:
        delete_from_elastic_by_source_filename(search_text, index_name, es_connection)
    else:
        update_or_remove_from_elastic(search_text, index_name, es_connection)

def delete_by_azure_container(date_to_process_from: datetime, hard_delete:bool, index_name:str, es_connection:Elasticsearch, product_area:str):
    # Ensure the input date is timezone-aware
    date_to_process_from = ensure_timezone_aware(date_to_process_from)
    
    container_name = 'sf-archived'
    prefix = f'{product_area}/Archive/'
    
    container_client = ContainerClient.from_connection_string(
        conn_str=AZURE_CONNECTION_STRING,
        container_name=container_name
    )
    
    blobs = container_client.list_blobs(name_starts_with=prefix)
    blobs_list = sorted(
        [blob for blob in blobs], 
        key=lambda x: x.last_modified,
        reverse=True
    )
    
    if len(blobs_list) == 0:
        print(f'No files found in container {container_name} with prefix {prefix}')
        return
        
    for blob in blobs_list:
        # Azure blob's last_modified is already timezone-aware (UTC)
        if blob.last_modified > date_to_process_from:
            print(f'Deleting using {blob.name} as search text')
            full_blob_path = f"{container_name}/{blob.name}"
            delete_by_search_text(full_blob_path, index_name, es_connection, hard_delete)
        else:
            print('Finished processing files modified after: ' + date_to_process_from.strftime('%Y-%m-%d %H:%M:%S %Z'))
            break

def run_delete(index_to_delete_from:str, hard_delete:bool, product_area:str, last_modified_date:datetime):
    elastic_cloud_id = os.environ['ELASTIC_CLOUD_ID']
    elastic_basic_auth = [os.environ['ELASTIC_USERNAME'], os.environ['ELASTIC_PASSWORD']]    
    logging.log(logging.INFO, f'elastic_cloud_id: [{elastic_cloud_id}]\telastic_basic_auth: [{str(elastic_basic_auth)}]')
    logging.log(logging.INFO, f'attempting to delete from index_name: [{index_to_delete_from}]')
    es_connection = Elasticsearch(cloud_id=elastic_cloud_id, basic_auth=elastic_basic_auth)
    delete_by_azure_container(last_modified_date, hard_delete, index_to_delete_from, es_connection, product_area)

def validate_delete_run(product_area: str):
    if product_area not in PRODUCT_AREAS:
        print(f'Invalid product area: {product_area}')
        logging.log(logging.ERROR, f'Invalid product area: {product_area}')            
        return False
    else:
        return True

def run_delete_for_all_product_areas(last_modified_date:datetime, hard_delete:bool):
    logging.log(logging.INFO, 'Running delete for all product areas')
    for product_area in PRODUCT_AREAS:
        product_area_message = f'Running delete for product area: {product_area}'
        print(product_area_message)
        logging.log(logging.INFO, product_area_message)
        
        for product_index in PRODUCT_INDEXES[product_area]:
            product_index_message = f'Running delete for index: {product_index}'
            print(product_index_message)
            logging.log(logging.INFO, product_index_message)
            
            run_delete(
                index_to_delete_from=product_index,
                hard_delete=hard_delete,
                product_area=product_area,
                last_modified_date=last_modified_date
            )

def single_delete_run(product_area:str, hard_delete:bool, last_modified_date:datetime):    
    if validate_delete_run(product_area):
        delete_run_parameters = f'Running delete with the following parameters: product_area: {product_area}, hard_delete: {hard_delete}, last_modified_date: {last_modified_date}'
        print(delete_run_parameters)
        logging.log(logging.INFO, delete_run_parameters)
        
        for product_index in PRODUCT_INDEXES[product_area]:
            run_delete(
                index_to_delete_from=product_index,
                hard_delete=hard_delete,
                product_area=product_area,
                last_modified_date=last_modified_date
            )
    else:
        print('Delete not run')