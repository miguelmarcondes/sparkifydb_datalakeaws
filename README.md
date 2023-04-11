# Data Lake Project from Udacity

Submission for the Udacity Data Engineering Nanodegree, Data Lake chapter.

## Summary

This project focuses on fetching json data from an AWS S3 bucket and processing it using Apache Spark in Python. The data is transformed and neatly organized using a star schema, making it available to queries focused on analytics.

## Data Folder

Sample data. The full data is available at s3://udacity-dend/song_data and s3://udacity-dend/log_data.

## dl.cfg

File used as a configuration file, having two variables used for credentials for the AWS environment:

1. AWS_ACCESS_KEY_ID
2. AWS_SECRET_ACCESS_KEY

## ETL

Run the script using:

```bash
python etl.py
```