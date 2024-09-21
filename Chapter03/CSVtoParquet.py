import boto3
import pandas as pd
from urllib.parse import unquote_plus
import os

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    glue = boto3.client('glue')
    
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])

        # Split the key to determine DB and table names
        key_list = key.split("/")
        db_name = key_list[-3]  # The third last element
        table_name = key_list[-2]  # The second last element

        input_path = f"s3://{bucket}/{key}"
        output_path = f"s3://dataeng-clean-zone-vrams/{db_name}/{table_name}/"

        # Load data using boto3
        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            input_df = pd.read_csv(response['Body'])  # Load CSV
            print(f"Loaded DataFrame from {input_path}")
        except Exception as e:
            print(f"Error loading data from S3: {e}")
            return {'statusCode': 500, 'body': f"Error loading data: {e}"}

        # Save DataFrame as Parquet to S3
        try:
            os.makedirs('/tmp/output', exist_ok=True)
            parquet_file = f"/tmp/{table_name}.parquet"
            input_df.to_parquet(parquet_file, index=False)

            # Upload the Parquet file to the output S3 path
            s3.upload_file(parquet_file, 'dataeng-clean-zone-vrams', f"{db_name}/{table_name}/{table_name}.parquet")
            print(f"Uploaded Parquet file to {output_path}")
        except Exception as e:
            print(f"Error saving Parquet file to S3: {e}")
            return {'statusCode': 500, 'body': f"Error saving Parquet file: {e}"}

        # Register the Parquet data in AWS Glue
        try:
            # Check if the database exists
            current_databases = glue.get_databases()
            if db_name not in [db['Name'] for db in current_databases['DatabaseList']]:
                print(f"- Database {db_name} does not exist... creating")
                glue.create_database(DatabaseInput={'Name': db_name})
            else:
                print(f"- Database {db_name} already exists")

            # Create or update the Glue table
            table_input = {
                'Name': table_name,
                'StorageDescriptor': {
                    'Columns': [{'Name': col, 'Type': 'string'} for col in input_df.columns],
                    'Location': output_path,
                    'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    'Compressed': False,
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                        'Parameters': {
                            'serialization.format': '1'
                        }
                    }
                },
                'TableType': 'EXTERNAL_TABLE'
            }

            # Attempt to create the table
            glue.create_table(DatabaseName=db_name, TableInput=table_input)
            print(f"Table {table_name} registered successfully in Glue catalog.")

        except glue.exceptions.AlreadyExistsException:
            print(f"Table {table_name} already exists in database {db_name}.")
        except Exception as e:
            print(f"Error creating table: {e}")
            return {'statusCode': 500, 'body': f"Error creating table: {e}"}

    return {
        'statusCode': 200,
        'body': f'Table {table_name} processing completed.'
    }

