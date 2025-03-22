import json
import boto3
import psycopg2
import csv
from io import StringIO
from etls.interact_with_db import create_rds_connection
from datetime import datetime
from utils.constants import AWS_ACCESS_KEY, AWS_BUCKET_NAME, AWS_ACCESS_KEY_ID

def get_data(conn):
    cur = conn.cursor()

    get_data_query = """
        SELECT * FROM reddit_posts 
        WHERE DATE(created_time) = CURRENT_DATE;
    """

    try:
        cur.execute(get_data_query)
        rows = cur.fetchall()
        column_names = [desc[0] for desc in cur.description]  # Lấy tên cột
        conn.commit()
        return rows, column_names

    except Exception as e:
        print(f"An error occurred: {e}")
        return [], []

def load_data_to_s3(rows, column_names):
    access_key = AWS_ACCESS_KEY_ID
    secret_access_key = AWS_ACCESS_KEY
    csv_buffer = StringIO()

    today = datetime.today().strftime('%Y-%m-%d')

    csv_writer = csv.writer(csv_buffer, quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(column_names)  # Ghi tên cột
    csv_writer.writerows(rows)  # Ghi dữ liệu

    s3_client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_access_key,
        region_name='us-west-2'
    )

    s3_bucket_name = AWS_BUCKET_NAME
    s3_file_path = f"datalake/raw_csv_file/reddit_posts_{today}.csv"

    try:
        s3_client.put_object(Bucket=s3_bucket_name, Key=s3_file_path, Body=csv_buffer.getvalue())
        print("Successfully pushed file into S3!")

    except Exception as e:
        print(f"An error occurred: {e}")

def load_from_db_to_s3():
    conn = create_rds_connection()
    rows, column_names = get_data(conn)
    if rows:
        load_data_to_s3(rows, column_names)
    else:
        print("No data retrieved.")

if __name__ == '__main__':
    load_from_db_to_s3()
