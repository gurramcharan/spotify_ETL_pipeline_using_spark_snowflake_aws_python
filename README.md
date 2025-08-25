# 🎧 Spotify End-to-End ETL Using AWS, Spark, Snowflake and Python

## 📌 Introduction
This project demonstrates a fully automated **ETL (Extract, Transform, Load)** pipeline using the **Spotify API** on **AWS Cloud**. The pipeline is designed to retrieve data about songs, albums, and artists from the Spotify API, process and transform the data using Spark, store the transformed data into AWS s3 and load it to Snowflake -- a data warehouse for querying and analytics.

## 🧩 Architecture
![Spotify Spark_ETL_Architecture](https://github.com/gurramcharan/spotify_ETL_pipeline_using_spark_snowflake_aws_python/blob/main/process%20flow%20diagram.jpg)

## 🔁 Project Execution Flow
**Extract data from Spotify API → Trigger Lambda function (every 1 hour) → Store raw data in S3 → Trigger AWS Glue script → Clean and format data using pyspark → Load transformed data into S3 → Snowpipe triggers → Converts the data into tables using Snowflake → Query using Snoflake**

## 📂 Dataset / API Details
We use the [Spotify Web API](https://developer.spotify.com/documentation/web-api), which provides structured metadata about:
- 🎤 Music Artists  
- 💽 Albums  
- 🎵 Songs  

## 🚀 AWS Services Used
- **Amazon S3**: Used as the main data lake to store raw and transformed JSON/CSV files.
- **AWS Lambda**: Serverless compute that handles scheduled data extraction and transformation logic.
- **Amazon CloudWatch**: Monitors the Lambda executions and logs performance, errors, and invocations.
- **AWS Glue Notebook**: Provides an interactive Jupyter-like environment to write, run, and debug PySpark/SQL code for data exploration, ETL development, and data transformation before automating the jobs.
- **Snowflake Snowpipe**: A continuous data ingestion service in Snowflake that automatically loads data from S3 into Snowflake tables in near real-time using event notifications or file polling.


## 🛠️ Packages Used
```bash
pandas
numpy
aws boto3
pyspark
aws glue
snowflake sql
```
