-- Create a table named 'images_s3' to store transformed data with specified columns and transformations.

-- Define the structure of the table with columns and data types, along with transformations.
CREATE TABLE images_s3 AS
SELECT 
    cd."RAW_DATA":"article_id"::INT AS ARTICLE_ID,  -- Transform the "article_id" column to INT datatype and alias it as ARTICLE_ID
    'https://h-and-m-kaggle-images.s3.us-west-2.amazonaws.com/' || cd."RAW_DATA":"article_id"::VARCHAR || '.jpg' AS S3_URL  -- Create S3 URL based on article_id

FROM 
    "EXPLORATION_DB"."HM_RAW"."IMAGES_TO_S3" as cd  -- Select data from the "IMAGES_TO_S3" table and alias it as "cd"

ORDER BY cd."ETL_TIMESTAMP" DESC;  -- Order the data by "ETL_TIMESTAMP" in descending order

-- Define the Kafka connector settings for the 'images_s3' table.
ALTER TABLE images_s3
SET (
  'connector' = 'kafka',  -- Use the Kafka connector
  'topic' = 'customers_topic',  -- Specify the Kafka topic name
  'properties.bootstrap.servers' = 'pkc-ymrq7.us-east-2.aws.confluent.cloud:9092',  -- Provide Kafka bootstrap server information
  'scan.startup.mode' = 'earliest',  -- Specify the behavior for reading data from the Kafka topic
  'properties.security.protocol' = 'SASL_SSL',  -- Set security protocol to SASL SSL
  'properties.sasl.mechanism' = 'PLAIN',  -- Specify SASL mechanism
  'properties.sasl.username' = 'XM66IMB4LT2RIOXY',  -- Provide SASL username
  'properties.sasl.password' = 'WgA9NJF4rM0xI+2EWuswmZGxBpW86K9rKNQyRBK5mS+9kU8IiASM+OrJSL4UpUG5'  -- Provide SASL password
) FORMAT PLAIN ENCODE JSON;
    
