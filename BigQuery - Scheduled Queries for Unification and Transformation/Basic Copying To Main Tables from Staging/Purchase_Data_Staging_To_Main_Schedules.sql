CREATE or Replace TABLE
customer-marketing-analytics.customer_and_marketing_data_analysis.Purchase_Data_CMA as


select purchase_id, customer_id, cast(purchase_date as date) as purchase_date,
quantity, payment_method, shipping_address,
 shipping_state, product_id from customer-marketing-analytics.customer_and_marketing_data.customer_purchases_data
