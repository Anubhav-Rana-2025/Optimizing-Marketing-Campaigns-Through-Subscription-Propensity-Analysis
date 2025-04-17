CREATE or Replace TABLE
customer-marketing-analytics.customer_and_marketing_data_analysis.Loyalty_Program_Data_CMA as
select customer_id, enrollment_status,
CAST ((CASE WHEN enrollment_date = "NA" then null else enrollment_date end) as DATE) as enrollment_date
from customer-marketing-analytics.customer_and_marketing_data.Loyalty_Program
