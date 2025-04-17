CREATE OR REPLACE TABLE customer-marketing-analytics.customer_and_marketing_data_analysis.Web_Analytics_Summary AS

-- Numerical Columns Summary
WITH numerical_col_summary AS (
  SELECT 
    customer_id, 
    AVG(session_duration) AS avg_session_duration, 
    AVG(page_views) AS avg_pg_views
  FROM customer-marketing-analytics.customer_and_marketing_data_analysis.Web_analytics_Data_CMA
  GROUP BY customer_id
),

-- Days since Last Visit Column
last_day_of_visit AS (
  SELECT 
    customer_id, 
    date_of_visit, 
    ROW_NUMBER() OVER(PARTITION BY CAST(customer_id AS INT) ORDER BY date_of_visit DESC) AS rnkdov
  FROM customer-marketing-analytics.customer_and_marketing_data_analysis.Web_analytics_Data_CMA
),

last_date_of_visit AS (
  SELECT 
    customer_id, 
    date_of_visit AS last_visit_date
  FROM last_day_of_visit
  WHERE rnkdov = 1
),

days_since_visit AS (
  SELECT 
    customer_id, 
    DATE_DIFF(DATE('2025-05-05'), date_of_visit, DAY) AS last_visit_in_days
  FROM last_day_of_visit
  WHERE rnkdov = 1
),

-- Most frequent traffic channel
traffic_counts AS (
  SELECT 
    customer_id, 
    traffic_Channel, 
    COUNT(*) AS counts
  FROM customer-marketing-analytics.customer_and_marketing_data_analysis.Web_analytics_Data_CMA
  GROUP BY customer_id, traffic_Channel
),

ranked_traffic_channels AS (
  SELECT 
    customer_id, 
    traffic_Channel, 
    ROW_NUMBER() OVER(PARTITION BY CAST(customer_id AS INT) ORDER BY counts) AS rnktc
  FROM traffic_counts
),

most_frequent_traffic_channel AS (
  SELECT 
    customer_id, 
    traffic_Channel
  FROM ranked_traffic_channels
  WHERE rnktc = 1
),

-- Most frequent device_type
device_counts AS (
  SELECT 
    customer_id, 
    device_type, 
    COUNT(*) AS counts
  FROM customer-marketing-analytics.customer_and_marketing_data_analysis.Web_analytics_Data_CMA
  GROUP BY customer_id, device_type
),

ranked_device_type AS (
  SELECT 
    customer_id, 
    device_type, 
    ROW_NUMBER() OVER(PARTITION BY CAST(customer_id AS INT) ORDER BY counts) AS rnkdc
  FROM device_counts
),

most_frequent_devivce_type AS (
  SELECT 
    customer_id, 
    device_type
  FROM ranked_device_type
  WHERE rnkdc = 1
),

-- Most frequent browser
browser_counts AS (
  SELECT 
    customer_id, 
    browser, 
    COUNT(*) AS counts
  FROM customer-marketing-analytics.customer_and_marketing_data_analysis.Web_analytics_Data_CMA
  GROUP BY customer_id, browser
),

ranked_browser AS (
  SELECT 
    customer_id, 
    browser, 
    ROW_NUMBER() OVER(PARTITION BY CAST(customer_id AS INT) ORDER BY counts) AS rnkbc
  FROM browser_counts
),

most_frequent_browser_type AS (
  SELECT 
    customer_id, 
    browser
  FROM ranked_browser
  WHERE rnkbc = 1
),

-- Most frequent event type
event_counts AS (
  SELECT 
    customer_id, 
    event_type, 
    COUNT(*) AS counts
  FROM customer-marketing-analytics.customer_and_marketing_data_analysis.Web_analytics_Data_CMA
  GROUP BY customer_id, event_type
),

ranked_events AS (
  SELECT 
    customer_id, 
    event_type, 
    ROW_NUMBER() OVER(PARTITION BY CAST(customer_id AS INT) ORDER BY counts) AS rnkec
  FROM event_counts
),

most_frequent_events AS (
  SELECT 
    customer_id, 
    event_type AS most_frequent_event
  FROM ranked_events
  WHERE rnkec = 1
),

-- Last landing page
last_landing_page AS (
  SELECT 
    customer_id, 
    landing_page AS last_landing_page
  FROM (
    SELECT 
      customer_id, 
      landing_page, 
      ROW_NUMBER() OVER(
        PARTITION BY CAST(customer_id AS INT) 
        ORDER BY COUNT(*) DESC, landing_page
      ) AS rnk
    FROM customer-marketing-analytics.customer_and_marketing_data_analysis.Web_analytics_Data_CMA a
    WHERE STRUCT(customer_id, date_of_visit) IN (
      SELECT AS STRUCT 
        customer_id, 
        date_of_visit 
      FROM last_day_of_visit 
      WHERE rnkdov = 1
    )
    GROUP BY customer_id, landing_page
  )
  WHERE rnk = 1
),

-- Most frequent loyalty interaction
pop_up_counts AS (
  SELECT 
    customer_id, 
    `Loyalty Offer Pop Up`, 
    COUNT(*) AS counts
  FROM customer-marketing-analytics.customer_and_marketing_data_analysis.Web_analytics_Data_CMA
  GROUP BY customer_id, `Loyalty Offer Pop Up`
),

ranked_puc AS (
  SELECT 
    customer_id, 
    `Loyalty Offer Pop Up`, 
    ROW_NUMBER() OVER(PARTITION BY CAST(customer_id AS INT) ORDER BY counts) AS rnkpuc
  FROM pop_up_counts
),

most_frequent_loyalty_interaction AS (
  SELECT 
    customer_id, 
    `Loyalty Offer Pop Up` AS most_frequent_pop_up_interaction
  FROM ranked_puc
  WHERE rnkpuc = 1
)

-- Final query
SELECT 
  a.customer_id, 
  avg_session_duration, 
  avg_pg_views, 
  last_visit_in_days, 
  traffic_Channel, 
  device_type, 
  browser, 
  most_frequent_event, 
  last_landing_page, 
  most_frequent_pop_up_interaction
FROM numerical_col_summary a
INNER JOIN most_frequent_traffic_channel b ON a.customer_id = b.customer_id
INNER JOIN most_frequent_events c ON b.customer_id = c.customer_id
INNER JOIN most_frequent_browser_type d ON c.customer_id = d.customer_id
INNER JOIN most_frequent_devivce_type e ON d.customer_id = e.customer_id
INNER JOIN days_since_visit f ON e.customer_id = f.customer_id
INNER JOIN last_landing_page g ON f.customer_id = g.customer_id
INNER JOIN most_frequent_loyalty_interaction h ON g.customer_id = h.customer_id;
