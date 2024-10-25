import os
import logging
from pathlib import Path
from datetime import datetime
from elasticsearch import Elasticsearch

ELASTIC_CLOUD_ID = os.environ['ELASTIC_CLOUD_ID']
ELASTIC_USERNAME = os.environ['ELASTIC_USERNAME']
ELASTIC_PASSWORD = os.environ['ELASTIC_PASSWORD']

PRODUCT_AREAS = ['ATO', 'BillBlast', 'Docketing', 'Expert', 'Handshake', 'iTimekeep', 'Rainmaker', 'vibyaderant', 'testing']
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




#Will delete all documents from the given index that have a source file_name that contains the value of file_name_to_delete
#or file_name_to_delete of expert/documentation will delete all documents from the index that are under the expert/documentation folder in azure
def delete_from_elastic_by_source_filename(file_name_to_delete:str, index_name:str, elastic_client:Elasticsearch):
  logging.log(logging.INFO, f'Running delete by query on index {index_name} for metadata.source.file_name {file_name_to_delete}')
  # The \" around file_name_to_delete makes it so it will only delete file_names that contain that exact text
  delete_query = {
    "query_string": {
      "default_field": "metadata.source.file_name", 
      "query": f"\"{file_name_to_delete}\"",
      "default_operator": "AND"
    }
  }
  logging.log(logging.INFO, f'Delete query: {delete_query}')
  #print(f'Delete query: {delete_query}')
  elastic_client.delete_by_query(index=index_name, query=delete_query)




def update_or_remove_from_elastic(search_text:str, index_name:str, elastic_client:Elasticsearch):
    #elastics default search size is 10, and max is 10000
    search_size=100
    logging.log(logging.INFO, f'Searching elastic index {index_name} for metadata.source.file_name {search_text}')
    search_response = elastic_client.search(index=index_name, size=search_size, query={
    "query_string": { "default_field": "metadata.source.file_name", "query": f"\"{search_text}\"", "default_operator": "AND" }
    })
    search_results = search_response.body['hits']['hits']
    for result in search_results:
        existing_id_with_hash = result["_id"]
        existing_source = result['_source']['metadata']['source']
        #if the result only has one source delete it, otherwise update the source removing the deleted entry
        for source in existing_source:
           if search_text in source:
               #remove all sources that contain the search_text
               existing_source.remove(source)
        if len(existing_source) <= 1:
            logging.log(logging.INFO, f'Deleting chunk with id: {existing_id_with_hash}')
            elastic_client.delete(index=index_name, id=existing_id_with_hash)
        else:
            update_script = {'source':"ctx._source.metadata.source = params.source", 'lang':'painless', 'params':{'source': existing_source}}
            elastic_client.update(index=index_name, id=existing_id_with_hash, script=update_script)
    #had a full page of results so need to run again to get the next page
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


def delete_by_directory(date_to_process_from: datetime, hard_delete:bool, index_name:str, es_connection:Elasticsearch, source_path:str):
    if source_path is None or source_path == '':
        source_empty_message = 'source_path cannot be empty'
        print(source_empty_message)
        logging.log(logging.ERROR, source_empty_message)
        return
    source_storage_path = Path(source_path)
    #Gets all files in the source path and sorts them by modified time in descending order so the most recent files are processed first
    files = sorted(source_storage_path.glob('**/*.json'), key=os.path.getmtime, reverse=True) # + list(storage_path.glob('**/*.csv'))
    if(len(files) == 0):
        print('No files found in the source path')
        return
    for file in files:
        modified_time = os.path.getmtime(file)
        modified_time = datetime.fromtimestamp(modified_time)
        #Only process files that have been modified after the date_to_process_from
        if modified_time > date_to_process_from:
            print('Deleting using ' + file.name + ' as search text')
            delete_by_search_text(file.name, index_name, es_connection, hard_delete)
        else:
            print('Finished processing files modified after: ' + date_to_process_from.strftime('%Y-%m-%d'))
            break


#setup for elastic
def run_delete(index_to_delete_from:str, delete_from_directory:bool, hard_delete:bool, search_text:str, source_path:str, last_modified_date:datetime):
    elastic_cloud_id = os.environ['ELASTIC_CLOUD_ID']
    elastic_basic_auth = [os.environ['ELASTIC_USERNAME'], os.environ['ELASTIC_PASSWORD']]    
    logging.log(logging.INFO, 'elastic_cloud_id: [' + elastic_cloud_id + ']\telastic_basic_auth: [' + str(elastic_basic_auth) + ']')
    logging.log(logging.INFO, 'attempting to delete from index_name: [' + index_to_delete_from + ']')
    es_connection = Elasticsearch(cloud_id = elastic_cloud_id, basic_auth = elastic_basic_auth)
    if delete_from_directory:
        delete_by_directory(last_modified_date, hard_delete, index_to_delete_from, es_connection, source_path)
    else:     
        delete_by_search_text(search_text, index_to_delete_from, es_connection, hard_delete)


def validate_single_delete_run(index_to_delete_from: str, delete_from_directory:bool, directory_source_path:str, search_text:str):
    if delete_from_directory:
        if directory_source_path is None or directory_source_path == '':
            source_empty_message = 'source_path cannot be empty'
            print(source_empty_message)
            logging.log(logging.ERROR, source_empty_message)            
            return False
        user_input_message = f'You are about to delete items from the elastic index {index_to_delete_from} based on the directory {directory_source_path} . Type continue to proceed?'
    else:
        if search_text is None or search_text == '':
            search_text_empty_message = 'search_text cannot be empty'
            print(search_text_empty_message)
            logging.log(logging.ERROR, search_text_empty_message)
            return False
        user_input_message = f'You are about to delete items from the elastic index {index_to_delete_from} based on the search text {search_text} . Type continue to proceed?'

    if input(user_input_message) != 'continue':
        return False
    return True


def single_delete_run(index_to_delete_from:str, delete_from_directory:bool, hard_delete:bool, search_text:str, source_path:str, last_modified_date:datetime):    
    validated = validate_single_delete_run(index_to_delete_from, delete_from_directory, source_path, search_text)
    if validated:
        delete_run_parameters = f'Running delete with the following parameters: index_to_delete_from: {index_to_delete_from}, delete_from_directory: {delete_from_directory}, hard_delete: {hard_delete}, search_text: {search_text}, source_path: {source_path}, last_modified_date: {last_modified_date}'
        print(delete_run_parameters)
        logging.log(logging.INFO, delete_run_parameters)     
        run_delete(
            index_to_delete_from=index_to_delete_from, 
            delete_from_directory=delete_from_directory, 
            hard_delete=hard_delete, 
            search_text=search_text, 
            source_path=source_path, 
            last_modified_date=last_modified_date
        )               
    else:
        print('Delete not run')


def run_delete_for_all_product_areas(last_modified_date:datetime, hard_delete:bool):
    delete_from_products_aread_message = f'You are about to delete items from elastic, based on the provided archived directory. Type continue to proceed?'
    if input(delete_from_products_aread_message) == 'continue':
        logging.log(logging.INFO, 'Running delete for all product areas')
        for product_area in PRODUCT_AREAS:
            product_area_message = f'Running delete for product area: {product_area}'
            print(product_area_message)
            logging.log(logging.INFO, product_area_message)
            for product_index in PRODUCT_INDEXES[product_area]:
                product_index_message = f'Running delete for index: {product_index}'
                print(product_index_message)
                logging.log(logging.INFO, product_index_message)
                product_archive_path = os.environ['ARCHIVE_STORAGE_PATH'].format(product_area=product_area)
                print(f'Product archive path: {product_archive_path}' )
                run_delete(
                    index_to_delete_from=product_index, 
                    delete_from_directory=True,
                    hard_delete=hard_delete, 
                    search_text=None, 
                    source_path=product_archive_path, 
                    last_modified_date=last_modified_date
                )
    else:
        print('Delete not run')

