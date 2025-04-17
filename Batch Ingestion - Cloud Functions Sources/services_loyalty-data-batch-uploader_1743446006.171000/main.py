
from google.cloud import bigquery
from google.cloud import secretmanager
import pandas as pd
import requests as req

def retrieve_secret(secret_name) :
  project_id ="customer-marketing-analytics"
  secret_manager_client_obj = secretmanager.SecretManagerServiceClient()
  secret_object=secret_manager_client_obj.access_secret_version(name=f"projects/{project_id}/secrets/{secret_name}/versions/latest")
  secret_payload = secret_object.payload.data.decode('UTF-8')
  return secret_payload

def loyalty_data_batch_uploader(request):
  api_key = retrieve_secret('API_Key')
  bigquery_client_obj = bigquery.Client()
  url = f'https://my.api.mockaroo.com/loyalty_program_data.json?key={api_key}'
  table_id = 'customer-marketing-analytics.customer_and_marketing_data.Loyalty_Program'

  try :
    response = req.get(url)
    response.raise_for_status()
    df = pd.DataFrame(response.json())
    schema_structure = [bigquery.SchemaField(col,"FLOAT") if df[col].dtype=="float64"
    else bigquery.SchemaField(col,"INTEGER") if df[col].dtype=="int64"
    else bigquery.SchemaField(col,"DATE") if df[col].dtype=="datetime64[ns]"
    else bigquery.SchemaField(col,"BOOL") if df[col].dtype=="bool"
    else bigquery.SchemaField(col,"STRING") if df[col].dtype =="object" or pd.api.types.is_categorical_dtype(df[col]) or pd.api.types.is_string_dtype(df[col])
    else bigquery.SchemaField(col,"STRING") for col in df.columns]

    job_config = bigquery.LoadJobConfig(schema = schema_structure, write_disposition= "WRITE_APPEND")
    res=bigquery_client_obj.load_table_from_dataframe(df, table_id, job_config=job_config)
    print(res)
    print("successfully loaded the data into bigquery")
    return ("success",200)
  except Exception as e:
      print(f"Encountered an error {e}. Check detailed logs")
      return ("error",500)

