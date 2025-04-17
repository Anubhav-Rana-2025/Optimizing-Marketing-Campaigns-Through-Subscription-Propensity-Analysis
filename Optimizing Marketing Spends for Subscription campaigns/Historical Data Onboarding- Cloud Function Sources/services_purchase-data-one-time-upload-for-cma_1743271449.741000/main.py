import functions_framework
functions_framework
from google.cloud import storage
from google.cloud import bigquery
import re
import datetime as dt
import pandas as pd

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]
    
    load_purchase_data_to_BQ(bucket,name)

def load_purchase_data_to_BQ (bucket_name,filename) :
    gcs_client_obj = storage.Client()
    bq_client_obj = bigquery.Client()
    table_id = 'customer-marketing-analytics.customer_and_marketing_data.customer_purchases_data'
    #temp file path to store the uploaded file
    current_year = dt.datetime.now().year
    pattern =rf'^Purchase_Data_{current_year}'
    if (re.match(pattern,filename)):
        temp_path = f'/tmp/{filename}'
        blob_file = gcs_client_obj.bucket(bucket_name).blob(filename)
        blob_file.download_to_filename(temp_path)
        df = pd.read_csv(temp_path)
        try :
            table =bq_client_obj.get_table('customer-marketing-analytics.customer_and_marketing_data.customer_purchases_data')
            job_configs = bigquery.LoadJobConfig(autodetect=True,write_disposition='WRITE_APPEND')
            try:
                bq_client_obj.load_table_from_dataframe(df,table_id,job_config =job_configs)
                print('successfully appended the data')
            except Exception as e:
                print(f'Error {e} appending the data check logs')

        except Exception as e:
            print(f'Encountered error {e}. Table may not exist. Creating the table. If any other error check logs')
            schema = [bigquery.SchemaField(col,"FLOAT64") if df[col].dtype=='float64' 
                      else bigquery.SchemaField(col,"INT64") if df[col].dtype=='int64' 
                      else bigquery.SchemaField(col,"BOOL") if df[col].dtype=='bool'
                      else bigquery.SchemaField(col,"DATE") if df[col].dtype=='datetime64[ns]'
                      else bigquery.SchemaField(col, "STRING") if df[col].dtype == 'object' or pd.api.types.is_string_dtype(df[col])
                      else bigquery.SchemaField(col,"STRING")  for col in df.columns]
            table = bigquery.Table(table_id,schema)
            try :
                bq_client_obj.create_table(table)
            except Exception as e:
                print(f'error {e} creating the table, check logs')
            try:
                job_configs = bigquery.LoadJobConfig(autodetect=True,write_disposition='WRITE_APPEND')
                bq_client_obj.load_table_from_dataframe(df,table_id,job_config=job_configs)
                print('Successfully created the table and loaded the data')
            except Exception as e:
                print(f'error {e} loading the data check logs')
    else :
        print('Invalid File/Filename')
    




    
