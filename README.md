Project 5: Data Pipelines with Airflow

Project Summary
The music streaming company, Sparkify wants to automate the loading of data into their warehouse and wants to use Airflow as their scheduler to automate this task.

In this project I have created a pipeline using Airflow python API. 
I've used plugin operators to load the raw data (event data, song data) from S3 buckets and staged them into AWS redshift cluster.
Once the staging is compeleted, the staged data is used to create fact tables as well as dimension tables.
Once the data is loaded into the tables, I have done data quality checks for the tables to make sure that the data is properly staged and fact tables as well as
dimension tables are created.

Steps to execute:
1. I have utilized the Udacity's workspace. So, I have created all my plugin operators, main pipeline script using Airflow's Python API.
2. Started the Airflow using the command /opt/airflow/start.sh
3. Created the AWS connections and redshift connections using the Airflow UI
4. Launched the DAG from the UI to see the data populate into the Redshift Cluster.