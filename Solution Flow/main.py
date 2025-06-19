from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from google.cloud import bigquery, secretmanager
from flask import request, make_response
import functions_framework

# --- Conversation Memory (optional) ---
from langchain.memory import ConversationBufferWindowMemory
memory = ConversationBufferWindowMemory(k=4)

# --- Secret Manager: Get Gemini API Key ---
secret_client = secretmanager.SecretManagerServiceClient()
secret_path = "projects/reference-tide-458016-b3/secrets/gemini-api-key/versions/latest"
secret_response = secret_client.access_secret_version(request={"name": secret_path})
key = secret_response.payload.data.decode("UTF-8")

# --- Initialize Gemini LLM ---
llm = ChatGoogleGenerativeAI(model='gemini-1.5-flash-latest', api_key=key)

# --- Initialize BigQuery ---
bq_client = bigquery.Client(project='reference-tide-458016-b3')
project_id = 'reference-tide-458016-b3'
table_id = 'product_description_generator.trial'
full_table_name = f"{project_id}.{table_id}"
schema = bq_client.get_table(table_id).schema

# --- Prompt Template ---
prompt_template = PromptTemplate(
    input_variables=["user_input", "schema", "table"],
    template=(
        "You are a BigQuery expert. Write a valid SQL query using the following table:\n"
        "Table: {table}\n"
        "Schema: {schema}\n\n"
        "User Request: {user_input}\n\n"
        "Only return the SQL query.Unless explicitly specified, do no exlude any columns in your query. Do not include explanations, markdown, or anything else."
    )
)

# --- Chain ---
chain = LLMChain(llm=llm, prompt=prompt_template)

# --- Function to run SQL and return CSV ---
def access_bigquery(ai_sql_query):
    ai_sql_query = ai_sql_query.strip().strip("```sql").strip("```").strip()
    query_job = bq_client.query(ai_sql_query)
    df = query_job.to_dataframe()
    return df.to_csv(index=False)

# --- Cloud Function Entry Point ---
@functions_framework.http
def agent(request):
    # --- Handle CORS Preflight ---
    if request.method == 'OPTIONS':
        response = make_response('', 204)
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'POST, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
        response.headers['Access-Control-Max-Age'] = '3600'
        return response

    # --- Set CORS headers for actual request ---
    response_headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
    }

    try:
        data = request.get_json()
        user_input = data['query']

        # --- Step 1: Generate SQL query using LLMChain ---
        sql_response = chain.run({
            "user_input": user_input,
            "schema": str(schema),
            "table": full_table_name
        })

        # --- Step 2: Run SQL on BigQuery and get CSV ---
        csv_data = access_bigquery(sql_response)

        # --- Step 3: Return CSV as file ---
        response = make_response(csv_data, 200)
        response.headers['Content-Type'] = 'text/csv'
        response.headers['Content-Disposition'] = 'attachment; filename=output.csv'
        for k, v in response_headers.items():
            response.headers[k] = v
        return response

    except Exception as e:
        error_response = make_response({'error': str(e)}, 500)
        for k, v in response_headers.items():
            error_response.headers[k] = v
        return error_response
