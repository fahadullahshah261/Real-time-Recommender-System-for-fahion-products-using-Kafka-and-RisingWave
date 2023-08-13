-- Create a new table named 'customers' with the specified columns.
CREATE TABLE customers (
    ACTIVE FLOAT,
    FN FLOAT,
    AGE FLOAT,
    club_member_status VARCHAR,
    customer_id VARCHAR,
    fashion_news_frequency VARCHAR,
    postal_code VARCHAR
)
-- Define the Kafka connector settings for the 'customers' table.
WITH (
  'connector' = 'kafka',
  'topic' = 'customers_topic',
  'properties.bootstrap.servers' = 'pkc-ymrq7.us-east-2.aws.confluent.cloud:9092',
  'scan.startup.mode' = 'earliest', 
  'properties.security.protocol' = 'SASL_SSL', 
  'properties.sasl.mechanism' = 'PLAIN', 
  'properties.sasl.username' = 'XM66IMB4LT2RIOXY', 
  'properties.sasl.password' = 'WgA9NJF4rM0xI+2EWuswmZGxBpW86K9rKNQyRBK5mS+9kU8IiASM+OrJSL4UpUG5'
) FORMAT PLAIN ENCODE JSON
AS
SELECT 
    -- get the columns we need based on NVIDIA previous experiments
    COALESCE(NULLIF(cd."RAW_DATA":"Active",''), 0.0)::FLOAT AS ACTIVE, 
    COALESCE(NULLIF(cd."RAW_DATA":"FN",''), 0.0)::FLOAT AS FN,
    COALESCE(NULLIF(cd."RAW_DATA":"age",''), 0.0)::FLOAT AS AGE,
    cd."RAW_DATA":"club_member_status"::VARCHAR AS club_member_status,
    cd."RAW_DATA":"customer_id"::VARCHAR AS customer_id,
    cd."RAW_DATA":"fashion_news_frequency"::VARCHAR AS fashion_news_frequency,
    cd."RAW_DATA":"postal_code"::VARCHAR AS postal_code
FROM 
     CUSTOMERS  as cd;

