--Create a streaming source for articles data from Kafka.

CREATE SOURCE articles (
  article_id VARCHAR,
  product_code VARCHAR,
  product_type_no VARCHAR,
  product_group_name VARCHAR, 
  graphical_appearance_no VARCHAR,
  colour_group_code VARCHAR,
  perceived_colour_value_id VARCHAR,
  perceived_colour_master_id VARCHAR,
  department_no VARCHAR,
  index_code VARCHAR, 
  index_group_no VARCHAR,
  section_no VARCHAR,
  garment_group_no VARCHAR
) WITH ( 
    connector = 'kafka',
    topic = 'articles_topic',
    properties.bootstrap.server = 'pkc-ymrq7.us-east-2.aws.confluent.cloud:9092',
    scan.startup.mode = 'earliest', 
    properties.security.protocol = 'SASL_SSL', 
    properties.sasl.mechanism = 'PLAIN', 
    properties.sasl.username = 'XM66IMB4LT2RIOXY', 
    properties.sasl.password = 'WgA9NJF4rM0xI+2EWuswmZGxBpW86K9rKNQyRBK5mS+9kU8IiASM+OrJSL4UpUG5'
) FORMAT PLAIN ENCODE JSON;

