# etl_pipeline_JSON_to_bigquery

# Introduction:
In this pipeline we are extracting data (sources: mongoDB or Apache Kafka or JSON file), transforming data and Loading it to bigquery.
The complete pipeline is created using GCP Free tier account.

# Resources:
** Data file location: Present in repository
** GCP account
** GCP services used:
    *** Dataflow notebook
    *** bigquery
    *** MongoDB

# Summary:
** Extract part of ETL is designed in such a way that it can read data from MongoDB, Apache Kafka or JSON file.
** Storing JSON in a variable 'df' and converting JSON to new line delimited JSON(NLDJ) as only New line delimited JSON can be inserted into bigquery.
** Creating dataset and table in bogquery.
** Loading data and creating table in bigquery.
** Running SQL query on bigquery table to flatten the semi structured data obtained.
** If needed Results can be printed using for loop.

# Author:
Puneet Parihar
