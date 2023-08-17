
CREATE TABLE filtered_dataframe_t AS
WITH frequent_customers AS (
    SELECT
        CUSTOMER_ID,
        COUNT(*) AS USER_INTERACTIONS
    FROM
        joined_dataframe_t
    WHERE
        T_DAT < '2020-09-08' -- Consider only the training set
    GROUP BY CUSTOMER_ID
    HAVING COUNT(*) >= 5 -- Minimum K interactions
)
SELECT
    t_s.*
FROM
    joined_dataframe_t AS t_s
JOIN
    frequent_customers AS f_c ON
    t_s.CUSTOMER_ID = f_c.CUSTOMER_ID;
