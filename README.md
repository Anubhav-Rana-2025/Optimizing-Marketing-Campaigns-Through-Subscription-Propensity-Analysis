# Optimizing Marketing Campaigns Through Subscription Propensity Analysis

## Project Context : Targeting The Right Audience For Greater Subscriptions And Optimized Spends

Shopley is an imaginary Retail Clothing Brand that sells apparels on its digital retail shop, Shopley.com. The company is a young start up, with a small customer base, but are trying to focus on driving higher revenue through a paid subscription model, SHOPLEY PRO , that offers customized apparels with customer requested designs at USD 15 dollars a month.

The Marketing team of the company is aiming to run a marketing campaign for the next 15 days.  
They would target the customers with a generalized campaign however they have access to direct to customer mail and hyper targeting tools that cost significantly more , and they wish to target only those who have high propensity to convert.

They wish to understand the right cohort to target to maintain a good cost to conversion. Moreover, they would like insights into customer behavior from web activity,  purchase and enrollment standpoint.

---

## Approach :

---

### Description

#### Historical Data Onboarding : [Folder Link ](https://github.com/Anubhav-Rana-2025/Optimizing-Marketing-Campaigns-Through-Subscription-Propensity-Analysis/tree/main/Historical%20Data%20Onboarding-%20Cloud%20Function%20Sources)

Google Cloud Storage buckets were created to enable marketers and CRM managers to upload historical data in excel format. Once the file is uploaded , the triggered Cloud Function script automates  a filename test. Once the test is passed, the Cloud Function then extracts the schema of the uploaded excel file and creates a BigQuery table, in case the table already exists in BigQuery, the data is appended.

In case of Schema mismatch, error and the error type is extracted written to logs.

---

#### Batch Data Ingestion : [Folder Link ](https://github.com/Anubhav-Rana-2025/Optimizing-Marketing-Campaigns-Through-Subscription-Propensity-Analysis/tree/main/Batch%20Ingestion%20-%20Cloud%20Functions%20Sources) 

The data was to be sourced from the Applications’ APIs ( Note that this are mock sources built on top of Mockaroo.com) . The API keys were stored in GCP Secret manager. A  Cloud Function was configured to call the APIs through Python’s request library. A Cron Job in Cloud Scheduler was created to trigger this Cloud Function every week on Monday at 9 pm. 

Service Accounts were configured to enable with required permissions to enable the cloud scheduler job and cloud function to work in tandem.

---

#### Data Processing and Storage On GCP : [Folder Link  ](https://github.com/Anubhav-Rana-2025/Optimizing-Marketing-Campaigns-Through-Subscription-Propensity-Analysis/tree/main/BigQuery%20-%20Scheduled%20Queries%20for%20Unification%20and%20Transformation)

The data was first made to land on a staging dataset with BigQuery. From there it was type casted to appropriate data types and passed on the Main Dataset.

The Master Table, Web Summary and Purchase Summary were created through multiple CTEs and subqueries and Joins using BigQuery Studio.

To Automate these Operations, BigQuery Scheduled Queries were used and configured to run a day after batch ingestion frequency.

---

#### Model Development : [Folder Link](https://github.com/Anubhav-Rana-2025/Optimizing-Marketing-Campaigns-Through-Subscription-Propensity-Analysis/tree/main/BigQuery%20-%20Scheduled%20Queries%20for%20Unification%20and%20Transformation)

An automated Model that could be trained on incremental data and log its accuracy metrics to a BigQuery table was developed on Vertex AI User Managed Instance. 

Gradient Boosting Classifier was chosen as the baseline model that ran the fastest and ensured optimized costs from cloud consumption standpoint.

Rolling Window Approach was used. The Training, Labelling, Validation Tests And Validation Label windows  were chosen as follows : 

1. Training Window : 1 Month of Active users in that Month.  
2. Training Target Labels : 1 Month at the  cut off of Training Window, people who enrolled labelled as 1 and those who did not labelled as 0.  
3. Test set : Active Individuals within 15 days post Training Target Window  
4. Test Target Labels : 15 days  post the end of Validation set window.

Final Model was trained on the most recent 30 days. 

The entire model development process developed in a modular manner that could be scheduled through Vertex AI notebook scheduler that enabled training on incremental data and future predictions.

---

#### Power BI Dashboard : Folder Link   

4 Dashboards were created:

1. **Shopley's Web Performance Analytics** : Provided an in depth look at customer Journey , Incoming Customer Traffic, enrollment analysis on the basis of  web activity etc.  
2. **Shopley's Pro Enrollment Analytics** : Provided in depth analysis of behavioral patterns of Enrolled and Not Enrolled customers and enabled tracking of conversion metrics.  
3. **Purchases Overview** : Provided an overview of purchase behavior from a perspective of different segments  
4. **Executive Overview** : A dashboard for quick overview of Key Enrollment , Web performance and purchase metrics that allow holistic high level view.
