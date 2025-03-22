# AWS Data pipeline using Reddit API (Social Platform)

<img src="[https://editorial.uefa.com/resources/028e-1b112bf31ef0-0dd2dd517d98-1000/ballon_d_or_photo.png](https://assets.euromoneydigital.com/dims4/default/7a47511/2147483647/strip/true/crop/840x472+0+0/resize/840x472!/quality/90/?url=http%3A%2F%2Feuromoney-brightspot.s3.amazonaws.com%2F73%2Ff8%2Fb8e0951f4e1db590cb614deba761%2Fnews-images-2025-01-08t103742-496.png)" alt="Amazon Web Service" width="300"/>

## Table of Contents
- [Introduction](#introduction)
- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [System Setup](#system-setup)
  
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
![images/system_architecture.png]

