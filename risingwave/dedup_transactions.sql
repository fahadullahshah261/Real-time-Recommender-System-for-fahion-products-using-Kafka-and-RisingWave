
CREATE TABLE dedup_transactions_t AS
SELECT
  t1.ARTICLE_ID,
  t1.customer_id,
  t2.price as price,
  t2.sales_channel_id as sales_channel_id,
  t1.t_dat as t_dat
FROM (
  SELECT DISTINCT
    ARTICLE_ID,
    customer_id,
    t_dat
  FROM transactions_t
) as t1
LEFT JOIN (
  SELECT
    ARTICLE_ID,
    customer_id,
    price,
    sales_channel_id,
    t_dat
  FROM transactions_t
) as t2
ON t1.ARTICLE_ID = t2.ARTICLE_ID
  AND t1.customer_id = t2.customer_id
  AND t1.t_dat <= t2.t_dat
ORDER BY
  t1.ARTICLE_ID,
  t1.customer_id,
  t1.t_dat;
