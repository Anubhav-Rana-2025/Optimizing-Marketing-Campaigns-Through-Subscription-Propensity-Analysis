import functions_framework
import pandas as pd
import requests as req
from google.cloud import bigquery
from google.cloud import secretmanager


@functions_framework.http
def hello_http(request):
    batch_load()


def secret_key_for_the_api(secret_name):
    secret_manager_client_obj = secretmanager.SecretManagerServiceClient()
    project_id = "customer-marketing-analytics"
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
   
    response = secret_manager_client_obj.access_secret_version(request={"name": secret_path})
    return response.payload.data.decode("UTF-8")


def batch_load() :
    table_id = 'customer-marketing-analytics.customer_and_marketing_data.customer_relationship_management_data'
    api_key = secret_key_for_the_api("API_Key")
    bigquery_client_obj = bigquery.Client()
    url = f'https://my.api.mockaroo.com/customer_relationship_management_system.json?key={api_key}'
    try :


        response = req.get(url, timeout=60)
        response.raise_for_status()


    except Exception as e:


        print(f'Error {e} getting data from source. Check logs for more info')
        return "Function Execution resulted in error" , 500


    try :
        data = response.json()
        df = pd.DataFrame(data)
        jobconfigs = bigquery.LoadJobConfig(autodetect=True, write_disposition= 'WRITE_APPEND')
        bigquery_client_obj.load_table_from_dataframe(df,table_id,job_config=jobconfigs)
        print('sucessfully loaded the data')
        return "Function Execution Successfully Complete" , 200
    except Exception as e :
        print(f'Error {e} loading the data. Check logs for more info') , 500
