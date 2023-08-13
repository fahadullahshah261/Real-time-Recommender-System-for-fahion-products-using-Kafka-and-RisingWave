-- This query identifies frequent customers based on their interactions and retrieves their data.

-- Common Table Expression (CTE) 'frequent_customers' calculates the count of interactions for each customer.
-- Only interactions before '2020-09-08' are considered (training set).
-- Customers with at least 5 interactions are selected.

WITH frequent_customers AS (
    SELECT 
        CUSTOMER_ID, 
        COUNT(*) AS USER_INTERACTIONS
    FROM 
        joined_dataframe
    WHERE 
        T_DAT < '2020-09-08' -- Consider only the training set
    GROUP BY CUSTOMER_ID
    HAVING USER_INTERACTIONS >= 5  -- Minimum K interactions
)
SELECT 
    t_s.*
FROM 
    joined_dataframe as t_s
JOIN 
    frequent_customers as f_c ON 
    t_s.CUSTOMER_ID = f_c.CUSTOMER_ID;
