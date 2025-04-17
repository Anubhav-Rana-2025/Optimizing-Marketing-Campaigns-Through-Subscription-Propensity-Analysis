CREATE OR REPLACE TABLE customer-marketing-analytics.customer_and_marketing_data_analysis.MASTER_CUSTOMER_TABLE AS

SELECT 
  a.customer_id, 
  age, 
  CASE 
    WHEN email IS NOT NULL THEN TRUE 
    ELSE FALSE 
  END AS email_provided, 
  gender, 
  income, 
  card_type, 
  state,
  avg_session_duration, 
  avg_pg_views, 
  traffic_Channel,
  most_frequent_event, 
  last_visit_in_days,
  most_frequent_pop_up_interaction, 
  last_landing_page, 
  device_type AS frequent_device_type, 
  browser AS frequent_browser_type

FROM customer-marketing-analytics.customer_and_marketing_data_analysis.Customer_Data_CMA a

LEFT JOIN customer-marketing-analytics.customer_and_marketing_data_analysis.Web_Analytics_Summary b 
  ON a.customer_id = b.customer_id

LEFT JOIN customer-marketing-analytics.customer_and_marketing_data_analysis.purchase_summary_CMA c 
  ON a.customer_id = c.customer_id

LEFT JOIN customer-marketing-analytics.customer_and_marketing_data_analysis.Loyalty_Program_Data_CMA d 
  ON a.customer_id = d.customer_id;
