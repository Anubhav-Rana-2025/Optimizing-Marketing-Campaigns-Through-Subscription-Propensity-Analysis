CREATE or Replace TABLE
customer-marketing-analytics.customer_and_marketing_data_analysis.Web_analytics_Data_CMA as


select customer_id, session_duration, page_views, session_id, traffic_Channel, device_type, browser, landing_page, `Loyalty Offer Pop Up`, user_location, event_type, cast(date_of_visit as date) as date_of_visit


from customer-marketing-analytics.customer_and_marketing_data.web_analytics_data
