--Create a streaming source for transaction data from Kafka.

CREATE source transactions (
  t_dat VARCHAR,
  customer_id VARCHAR,
  article_id VARCHAR,
  price VARCHAR,
  sales_channel_id VARCHAR
)
WITH (
  connector = 'kafka',
  topic = 'transactions_topic',
  properties.bootstrap.server = 'pkc-ymrq7.us-east-2.aws.confluent.cloud:9092',
  scan.startup.mode = 'earliest', 
  properties.security.protocol = 'SASL_SSL', 
  properties.sasl.mechanism = 'PLAIN', 
  properties.sasl.username = 'XM66IMB4LT2RIOXY', 
  properties.sasl.password = 'WgA9NJF4rM0xI+2EWuswmZGxBpW86K9rKNQyRBK5mS+9kU8IiASM+OrJSL4UpUG5'
) FORMAT PLAIN ENCODE JSON;
