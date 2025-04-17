Optimizing Marketing Campaigns Through Subscription Propensity Analysis

Project Context : Targeting The Right Audience For Greater Subscriptions And Optimized Spends
Shopley is an imaginary Retail Clothing Brand that sells apparels on its digital retail shop, Shopley.com. The company is a young start up, with a small customer base, but are trying to focus on driving higher revenue through a paid subscription model, SHOPLEY PRO , that offers customized apparels with customer requested designs at USD 15 dollars a month.

The Marketing team of the company is aiming to run a marketing campaign for the next 15 days. 
They would target the customers with a generalized campaign however they have access to direct to customer mail and hyper targeting tools that cost significantly more , and they wish to target only those who have high propensity to convert.



They wish to understand the right cohort to target to maintain a good cost to conversion. Moreover, they would like insights into customer behavior from web activity,  purchase and enrollment standpoint.

Approach : 


 

Description

Historical Data Onboarding : Folder Link 

Google Cloud Storage buckets were created to enable marketers and CRM managers to upload historical data in excel format. Once the file is uploaded , the triggered Cloud Function script automates  a filename test. Once the test is passed, the Cloud Function then extracts the schema of the uploaded excel file and creates a BigQuery table, in case the table already exists in BigQuery, the data is appended.

In case of Schema mismatch, error and the error type is extracted written to logs.


Batch Data Ingestion : Folder Link  

The data was to be sourced from 


