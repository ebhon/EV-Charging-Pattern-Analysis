from elasticsearch import Elasticsearch
import pandas as pd
import json

'''
This function is used to post our data into elasticsearch.
First we pulled our cleaned data using xcom_pull, then we convert to dataframe using pd.read_json since our data is in JSON formats.

Then we set our local elasticsearch url, and use "cleaned_data_index" as index_name

Then we do some check, if index_name is not exist on elastic search, we create new index using es.indices.create with index name as arguments

then we loop each row, to post our dataframe into elasticsearch. we used indexing, and doc_id is set on user_id column, this tells elastic that this column is primary key
so our data won't be duplicated on elastic search
'''

def post_to_elasticsearch(**kwargs):
    ti = kwargs['ti']
    cleaned_json_data = ti.xcom_pull(task_ids='cleaning_data', key='cleaned_dataframe_json')
    df = pd.read_json(cleaned_json_data, orient='records', lines=True)

    elasticsearch_url = "http://elasticsearch:9200"
    es = Elasticsearch(hosts=[elasticsearch_url])

    index_name = 'cleaned_data_index'

    if not es.indices.exists(index = index_name):
        es.indices.create(index = index_name)

    for idx, row in df.iterrows():
        doc = row.to_dict()
        doc_id = f"{row['user_id']}" 
        es.index(index = index_name, id=doc_id, body=doc) 
    print (f"Data successfully posted to Elasticsearch Index '{index_name}")