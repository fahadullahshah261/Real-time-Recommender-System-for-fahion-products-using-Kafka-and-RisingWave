
CREATE TABLE customers_T AS
SELECT
  COALESCE(CAST(NULLIF(active, '') AS FLOAT), 0.0) AS active,
  COALESCE(CAST(NULLIF(fn, '') AS FLOAT), 0.0) AS fn,
  COALESCE(CAST(NULLIF(age, '') AS FLOAT), 0.0) AS age,
  club_member_status,
  customer_id,
  fashion_news_frequency,
  postal_code
FROM customers;
