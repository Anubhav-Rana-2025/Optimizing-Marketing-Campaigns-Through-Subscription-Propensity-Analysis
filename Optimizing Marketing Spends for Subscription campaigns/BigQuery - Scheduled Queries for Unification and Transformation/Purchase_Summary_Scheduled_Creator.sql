CREATE OR REPLACE TABLE customer-marketing-analytics.customer_and_marketing_data_analysis.purchase_summary_CMA AS

WITH base_data AS (
  SELECT 
    CAST(customer_id AS INT) AS customer_id,
    purchase_date, 
    quantity, 
    payment_method, 
    product_id
  FROM customer-marketing-analytics.customer_and_marketing_data_analysis.Purchase_Data_CMA
),

-- Most Frequent Payment Method
Payment_Method_counts AS (
  SELECT 
    customer_id,
    payment_method, 
    COUNT(*) AS counts
  FROM base_data
  GROUP BY customer_id, payment_method
),

top_payment_method AS (
  SELECT 
    customer_id,
    payment_method
  FROM (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY counts DESC, payment_method ASC) AS rnk
    FROM Payment_Method_counts
  )
  WHERE rnk = 1
),

-- Most product_id Type
product_id_counts AS (
  SELECT 
    customer_id, 
    product_id, 
    COUNT(*) AS counts
  FROM base_data
  GROUP BY customer_id, product_id
),

top_product_id AS (
  SELECT 
    customer_id, 
    product_id
  FROM (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY counts DESC, product_id ASC) AS rnk
    FROM product_id_counts
  )
  WHERE rnk = 1
),

-- Last purchase Date
last_date_of_purchase AS (
  SELECT 
    customer_id, 
    MAX(purchase_date) AS last_purchased
  FROM base_data
  GROUP BY customer_id
),

purchase_diff AS (
  SELECT 
    customer_id, 
    DATE_DIFF(DATE('2025-05-05'), last_purchased, DAY) AS last_purchased_in_days
  FROM last_date_of_purchase
),

-- Summarized numerical vals
summary_tables AS (
  SELECT 
    customer_id, 
    AVG(quantity) AS avg_quantity
  FROM base_data
  GROUP BY customer_id
)

-- Final Query
SELECT 
  s.customer_id, 
  s.avg_quantity, 
  tpm.payment_method AS most_frequent_payment_method, 
  pdiff.last_purchased_in_days
FROM summary_tables s
INNER JOIN top_payment_method tpm ON s.customer_id = tpm.customer_id
INNER JOIN top_product_id tpi ON s.customer_id = tpi.customer_id
INNER JOIN purchase_diff pdiff ON s.customer_id = pdiff.customer_id;
