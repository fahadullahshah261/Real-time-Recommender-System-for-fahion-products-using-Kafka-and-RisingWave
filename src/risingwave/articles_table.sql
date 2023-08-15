-- This code creates a table named 'articles' with the specified columns and options.

-- Define the structure of the table with columns and data types.
CREATE TABLE articles (
  article_id INT, 
  product_code INT, 
  product_type_no INT, 
  product_group_name VARCHAR, 
  graphical_appearance_no INT, 
  colour_group_code INT, 
  perceived_colour_value_id INT, 
  perceived_colour_master_id INT, 
  department_no INT,
  index_code VARCHAR, 
  index_group_no INT, 
  section_no INT, 
  garment_group_no INT
) WITH ( 
  -- Specify that the table is being created using a Kafka connector.

  -- Specify the Kafka topic to connect to.
  connector = 'kafka',
  topic = 'articles_topic',
  
  -- Provide Kafka bootstrap server information.
  properties.bootstrap.server = 'pkc-ymrq7.us-east-2.aws.confluent.cloud:9092',
  
  -- Specify the behavior for reading data from the Kafka topic.
  scan.startup.mode = 'earliest', 
  
  -- Specify security protocol and credentials for connecting to Kafka.
  properties.security.protocol = 'SASL_SSL', 
  properties.sasl.mechanism = 'PLAIN', 
  properties.sasl.username = 'XM66IMB4LT2RIOXY', 
  properties.sasl.password = 'WgA9NJF4rM0xI+2EWuswmZGxBpW86K9rKNQyRBK5mS+9kU8IiASM+OrJSL4UpUG5'
) FORMAT PLAIN ENCODE JSON;
