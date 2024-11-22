import pandas as pd

'''
This function is used to export our cleaned data into csv.
First we need to pull our cleaned data using xcom_pull, then convert to dataframe using pd.read_json since our file is in JSON formats

Then we set our save path, in this case i'll save cleaned_data.csv into dags folder
then we save our dataframe into csv using df.to_csv
'''

def export_to_csv(**kwargs):
    ti = kwargs['ti']
    cleaned_json_data = ti.xcom_pull(task_ids='cleaning_data', key='cleaned_dataframe_json')

    df = pd.read_json(cleaned_json_data, orient='records', lines=True)

    file_path = '/opt/airflow/dags/P2M3_handwitanto_abraham_data_clean.csv'
    df.to_csv(file_path, index=False)
    print(f"Data Exported to {file_path}")