-- This query retrieves data from the 'dedup_transactions', 'articles_metadata', and 'customers_staging' tables and performs multiple JOIN operations.

-- Select all columns from the 'articles_metadata' table and all columns from the 'customers_staging' table.
-- Additionally, select 'price', 'sales_channel_id', and 't_dat' columns from the 'dedup_transactions' table.

SELECT 
    a_s.*,
    c_s.*,
    t_s.price,
    t_s.sales_channel_id,
    t_s.t_dat
FROM 
    dedup_transactions as t_s
JOIN 
    articles_metadata as a_s
ON 
    a_s.ARTICLE_ID = t_s.ARTICLE_ID
JOIN 
    customers_staging as c_s
ON
    c_s.CUSTOMER_ID = t_s.CUSTOMER_ID;
