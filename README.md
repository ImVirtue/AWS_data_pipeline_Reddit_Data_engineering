# AWS Data pipeline using Reddit API (Social Platform)

<img src="images/download.png" width="300">

## Table of Contents
- [Introduction](#introduction)
- [Overview](#overview)
- [Architecture](#architecture)
- [Result](#result)
  
## Introduction
This project provides a comprehensive data pipeline solution to extract, transform, and load (ETL) Reddit data into a Redshift data warehouse.
The pipeline leverages a combination of tools and services including Apache Airflow, Celery, PostgreSQL, Amazon S3, AWS Glue, Amazon Athena, and Amazon Redshift. 

## Overview

The pipeline is designed to:

1. Extract data from Reddit using its API.
2. Transform data and load it into Amazon RDS PostgreSQL database from Airflow.
3. Load data from RDS database into an S3 bucket from Airflow.
4. Transform the data using AWS Glue and Amazon Athena.
5. Load the transformed data into Amazon Redshift for analytics and querying.

## Architecture
![image](images/system_architecture.png)
1. **Reddit API**: Source of the data.
2. **Apache Airflow**: Orchestrates the ETL process and manages task distribution.
3. **RDS PostgreSQL**: Storage data from Reddit API.
4. **Amazon S3**: Raw data storage.
5. **AWS Glue**: Data cataloging and ETL jobs.
6. **Amazon Athena**: SQL-based data transformation.
7. **Amazon Redshift**: Data warehousing and analytics.

## Result
**Redshift:**
![image](images/redshift.png)

**Athena:**
![image](images/Athena.png)

