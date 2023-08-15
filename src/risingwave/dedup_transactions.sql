-- This query calculates the last values of specific columns within partitions of ARTICLE_ID and CUSTOMER_ID.


SELECT 
    ARTICLE_ID, 
    customer_id,
    last_value(price) OVER (PARTITION BY ARTICLE_ID, CUSTOMER_ID ORDER BY t_dat ASC) as price,
    last_value(sales_channel_id) OVER (PARTITION BY ARTICLE_ID, CUSTOMER_ID ORDER BY t_dat) as sales_channel_id,
    last_value(t_dat) OVER (PARTITION BY ARTICLE_ID, CUSTOMER_ID ORDER BY t_dat ASC) as t_dat
FROM 
    transactions_s3 as ts  -- Source table is 'transactions_s3'

-- Group the result by ARTICLE_ID, customer_id, price, sales_channel_id, and t_dat.
GROUP BY 
   ARTICLE_ID, 
   customer_id,
   price,
   sales_channel_id,
   t_dat

-- Order the result by ARTICLE_ID and customer_id.
ORDER BY 
    ARTICLE_ID, 
    customer_id;

