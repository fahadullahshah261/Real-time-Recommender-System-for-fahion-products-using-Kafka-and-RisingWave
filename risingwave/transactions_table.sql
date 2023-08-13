CREATE TABLE transactions_s3 AS
SELECT
    cd."RAW_DATA":"article_id"::INT AS ARTICLE_ID,
    cd."RAW_DATA":"customer_id"::VARCHAR AS customer_id,
    cd."RAW_DATA":"price"::FLOAT AS price,
    cd."RAW_DATA":"sales_channel_id"::INT AS sales_channel_id,
    cd."RAW_DATA":"t_dat"::DATETIME AS t_dat
FROM
    "EXPLORATION_DB"."HM_RAW"."TRANSACTIONS_TRAIN" as cd
ORDER BY t_dat ASC;

-- Define the Kafka connector settings for the 'transactions_s3' table.
ALTER TABLE transactions_s3
SET (
  'connector' = 'kafka',
  'topic' = 'transactions_topic',
  'properties.bootstrap.servers' = 'pkc-ymrq7.us-east-2.aws.confluent.cloud:9092',
  'scan.startup.mode' = 'earliest', 
  'properties.security.protocol' = 'SASL_SSL', 
  'properties.sasl.mechanism' = 'PLAIN', 
  'properties.sasl.username' = 'XM66IMB4LT2RIOXY', 
  'properties.sasl.password' = 'WgA9NJF4rM0xI+2EWuswmZGxBpW86K9rKNQyRBK5mS+9kU8IiASM+OrJSL4UpUG5'
) FORMAT PLAIN ENCODE JSON;
