
CREATE TABLE joined_dataframe_t AS
SELECT
    a_s.*,
    c_s.*,
    t_s.price,
    t_s.sales_channel_id,
    t_s.t_dat
FROM
    dedup_transactions_t as t_s
JOIN
    articles_metadata_t as a_s
ON
    a_s.ARTICLE_ID = t_s.ARTICLE_ID
JOIN
    customers_t as c_s
ON
    c_s.CUSTOMER_ID = t_s.CUSTOMER_ID;
