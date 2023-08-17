
CREATE TABLE transactions_t AS
SELECT
    CAST(t_dat AS DATE),
    customer_id,
    CAST(article_id AS INT) AS article_id,
    CAST(price AS DECIMAL) AS price,
    CAST(sales_channel_id AS INT) AS sales_channel_id
FROM
    transactions
    order by t_dat;
