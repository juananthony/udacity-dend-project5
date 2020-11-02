# Udacity DEND Project 4: Data Pipelines with Airflow
This is the fourth project in Udacity's Data Engineering Nanodegree. It is a data pipeline based on Apache Airflow. 

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Datasets

There is two datasets in S3:
* Log data: ```s3://udacity-dend/log_data```
* Song data: ```s3://udacity-dend/song_data```

