import csv
import json
import os
from confluent_kafka import Producer

from dotenv import load_dotenv

# Load environment variables from local.env file
load_dotenv('local.env')

def produce_csv_to_kafka(producer, csv_file_path, kafka_topic,batch_size):
        batch = []
        with open(csv_file_path, 'r') as csvfile:
          reader = csv.reader(csvfile)
          header_row = next(reader) # Get the header row
       
          
          for row in reader:
            json_data = {}
            for i, key in enumerate(header_row):
                json_data[key] = row[i]
           
            batch.append(json_data)

            if len(batch) == batch_size:
            
              for json_data_row in batch:
                 producer.produce(kafka_topic, value= json.dumps(json_data_row).encode('utf-8'))
              print(f"Produced batch of {len(batch)} messages to Kafka topic:", kafka_topic)
              batch=[]
              producer.flush()

          # Produce the remaining items in the batch
          for json_data_row in batch:
            producer.produce(kafka_topic, value=json.dumps(json_data_row).encode('utf-8'))
          producer.flush()
   
if __name__ == "__main__":
    # Read Confluent Cloud configuration
    kafka_conf = {
        'bootstrap.servers': os.getenv('BOOTSTRAP SERVERS'),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.getenv('API KEY'),
        'sasl.password':os.getenv( 'SECRET KEY'),
        'batch.num.messages': 1000000,  # Set batch size
        # 'linger.ms': 10000
    }
    producer = Producer(kafka_conf)

    # Define a list of configurations for different CSV-Kafka operations
    operations = [
    {
        'csv_file_path': '/root/customers.csv',
        'kafka_topic': 'customers_topic',
    },
    {
        'csv_file_path': '/root/articles.csv',
        'kafka_topic': 'articles_topic',
    },
    {
        'csv_file_path': '/root/url_of_images.csv',  
        'kafka_topic': 'images_topic',
    },
    {
        'csv_file_path': '/root/transactions_train.csv',  
        'kafka_topic': 'transactions_topic',
    }
    ]

    batch_size=1000
    for operation in operations:
        csv_file_path = operation['csv_file_path']
        kafka_topic = operation['kafka_topic']
        # Produce data to the specified Kafka topic
        produce_csv_to_kafka(producer, csv_file_path, kafka_topic, batch_size)   
   
    producer.flush()
    print('All data has been produced to Kafka.')
