'''
========================================
Milestone 3

Nama : Handwitanto Abraham
Batch : RMT036

File ini dibuat untuk mengelola data secara otomatis menggunakan airflow, sekaligus meng-export kembali data yang sudah di olah kedalam .csv dan menimpan juga dalam elasticsearch
========================================
'''


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum

#import function on a separate file (not on this python file)
from plugins.load_data import load_data
from plugins.clean_data import clean_data
from plugins.export_to_csv import export_to_csv
from plugins.post_to_elastic import post_to_elasticsearch

elasticsearch_url = "http://elasticsearch:9200"
local_tz = pendulum.timezone("Asia/Jakarta")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 7, tzinfo=local_tz),
    'retries': 1,
}

with DAG(
    'project_m3_pipeline',
    default_args=default_args,
    description='Pipeline to fetch, clean, export, and post to elastic',
    schedule_interval="30 6 * * *",
    catchup=False,
) as dag:
    
    fetch_data = PythonOperator(
        task_id='fetching_data',
        python_callable=load_data,
        do_xcom_push = True,
        dag = dag
    )

    clean_data_task = PythonOperator(
        task_id='cleaning_data',
        python_callable=clean_data,
        provide_context=True,
        dag=dag
    )

    export_to_csv_task = PythonOperator(
        task_id='export_to_csv',
        python_callable=export_to_csv,
        provide_context=True,
        dag=dag
    )

    post_to_elastic_task = PythonOperator(
        task_id='post_to_elasticsearch',
        python_callable=post_to_elasticsearch,
        provide_context=True,
        op_kwargs={"elasticsearch_url": elasticsearch_url},
        dag=dag,
    )

    fetch_data >> clean_data_task >> export_to_csv_task >> post_to_elastic_task