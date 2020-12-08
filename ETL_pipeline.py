#importing necessary files
import sys
import json
import os
import dns
import pymongo
from bson.json_util import dumps
from google.cloud import bigquery
from kafka import KafkaConsumer, KafkaProducer

#choosing source type
# Please pass 1,2 or 3 to choose different source types
print('Choose source type(choose any integer) :' + '\n' + '1.JSON' + '\n' + '2.MongoDB' + '\n' + '3.Kafka')
source_type = sya.argv[0]

# Reading data on the basis of source type
df = {}
# if source is json file
if source_type == 1:
	#Json file is present in current working directory
    with open('sample_events.json','r') as sample_events:
		#converting json to newline delimited json
        os.system("cat sample_events | jq -c '.[]'")
		#storing json in a variable
        df = json.load(sample_events)

#if source type is MongoDB
elif source_type ==2:
	#mongoDB credentials comes from MongoDB instance created while writing this code
    client_mongo = pymongo.MongoClient("mongodb+srv://pparihar94:ncn#400rZL@cluster0.kllio.mongodb.net/db?retryWrites=true&w=majority")
    db = client_mongo.db
    sample_events = dumps(db.sample_events.find(),default=json_util.default)
    os.system("cat sample_events | jq -c '.[]'")
    df = json.loads(sample_events)

#if source type is kafka
## not very sure about kafka logic
## could contain error in reading data from kafka code
elif source_type = 3:
    consumer = KafkaConsumer(bootstrap_servers='victoria.com:6667',auto_offset_reset='earliest',value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    consumer.subscribe(['my-topic'])
    sample_events = json.loads(consumer)
    os.system("cat sample_events | jq -c '.[]'")
    df = json.loads(sample_events)
	

#creating dataset in bigquery
# Constructing a BigQuery client object.
client = bigquery.Client()

#creating dataset ID
dataset_id = "{}.skuad".format(client.project)

# Construct a full Dataset object to send to the API.
dataset = bigquery.Dataset(dataset_id)

#Specifying the geographic location where the dataset should reside.
dataset.location = "US"

# Send the dataset to the API for creation, with an explicit timeout.
# Raises google.api_core.exceptions.Conflict if the Dataset already
# exists within the project.
dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

#generating configuration for table creation and data load
#table_id name comes from pipeline which was created on GCP while writing this code
#sample table_id
table_id = "etl-project-287211.skuad.raw_event_l0" 
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,autodetect = True
)
uri = df
#loading data from json and creating table
load_job = client.load_table_from_json(
    uri,
    table_id
)
load_job.result()  # Waits for the job to complete.

destination_table = client.get_table(table_id)
print("Loaded {} rows.".format(destination_table.num_rows))

#Flattening data on raw_event_lo table 
#Query_used
query = """SELECT items, 
       event_dimensions, 
       traffic_source, 
       device, 
       user_ltv, 
       user_first_touch_timestamp, 
       event_previous_timestamp, 
       val.KEY                        AS user_properties_key, 
       val.value.set_timestamp_micros AS user_properties_set_timestamp_micros, 
       val.value.double_value         AS user_properties_double_value, 
       val.value.float_value          AS user_properties_float_value, 
       val.value.int_value            AS user_properties_int_value, 
       val.value.string_value         AS user_properties_string_value, 
       user_pseudo_id, 
       event_bundle_sequence_id, 
       event_value_in_usd, 
       platform, 
       event_server_timestamp_offset, 
       ecommerce, 
       user_id, 
       app_info, 
       event_date, 
       event_name, 
       geo, 
       geo.metro, 
       val2.KEY                AS event_params_key, 
       val2.value.double_value AS event_params_double_value, 
       val2.value.float_value  AS event_params_float_value, 
       val2.value.int_value    AS event_params_int_value, 
       val2.value.string_value AS event_params_string_value, 
       stream_id, 
       event_timestamp 
FROM   `etl-project-287211.skuad.raw_event_l0` t, 
       unnest(user_properties) AS val, 
       unnest(event_params)    AS val2"""

#running query
query_job = client.query(query)
query_job.result()

#if needed query output can be generated using for loop
for i in query_job:
    print(i[:])
    break