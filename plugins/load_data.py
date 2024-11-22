import psycopg2
import pandas as pd

'''
This function is to fetch data from postgresql through docker
First we make a connection to postgresql using 'conn' variable.
once connection established, we run pandas read_sql sintax using 'query' and 'conn' arguments
after we pull our data, we close the connection to postgresql using 'conn.close()'

since xcom will be used to transfer data for different tasks, we need to convert our data into JSON using df.to_json.
Then push to xcom with key 'dataframe_json' 
'''
def load_data(**kwargs):
    conn = psycopg2.connect(
        host = 'postgres',
        port = 5432,
        dbname = 'airflow',
        user = 'airflow',
        password = 'airflow'
    )
    query = "SELECT * FROM table_m3;"
    df = pd.read_sql(query, conn)
    conn.close()

    json_data = df.to_json(orient='records', lines=True)

    kwargs['ti'].xcom_push(key='dataframe_json', value=json_data)