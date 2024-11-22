import pandas as pd

'''
This function is to do data_cleaning from the data that we fetch using load_data function.

Since our data is transferred using xcom, we need to pull our data using xcom_pull with tasks_id='fetching_data', this tasks_ids are what we used on airflow to fetch data
Then we convert our data back to dataframe using pd.read_json so we can do data cleanings

Data cleaning itself will do :
On Columns
- change any uppercase letter into lowercase
- replace whitespace into underscore
- remove special characters
- remove underscore at the start and end of column name

On Data
- Drop duplicated data
- Handling missing value (mode for string, and median for numerical)

Once we cleaned our data, we convert our processed data back to JSON so we can do xcom_push again, and this data can be used later on export_to_csv function or post_to_elastic function

'''

def clean_data(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='fetching_data', key='dataframe_json')

    df = pd.read_json(json_data, orient='records', lines=True)

    df.columns = (df.columns
                  .str.lower()
                  .str.replace(' ', '_')
                  .str.replace(r'[^a-z0-9_]', '', regex=True)
                  .str.strip('_')
    )

    df.drop_duplicates(inplace=True)

    for column in df.columns:
        if df[column].dtype == 'object':
            mode_val = df[column].mode()[0]
            df[column].fillna(mode_val, inplace=True)
        else:
            df[column].fillna(df[column].median(), inplace=True)
    
    cleaned_json_data = df.to_json(orient='records', lines=True)
    print (f"Cleaning data : Pushing cleaned data with {len(df)} rows")
    ti.xcom_push(key='cleaned_dataframe_json', value=cleaned_json_data)