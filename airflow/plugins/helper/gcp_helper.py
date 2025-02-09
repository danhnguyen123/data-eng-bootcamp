import json
from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import pandas_gbq
from config import config
from logging import Logger

class GCSHelper:

    def __init__(self, client=None, credentials_file=None, bucket_name=None):

        self.client = client
        self.bucket_name = bucket_name
        if not self.client:
            self.credentials = service_account.Credentials.from_service_account_file(credentials_file or config.SERVICE_ACCOUNT)
            self.client = storage.Client(credentials=self.credentials)
            self.bucket = self.client.get_bucket(self.bucket_name)

    def upload_json(self, json_string, file_name):

        # Get the specified bucket
        

        # Create a blob with the desired file name
        blob = self.bucket.blob(file_name)

        # Upload the JSON string to the blob
        try:
            blob.upload_from_string(json_string, content_type='application/json; charset=utf-8')
            print(f"JSON file uploaded to GCS: gs://{self.bucket_name}/{file_name}")
        except:
            print(f"Error when uploading to gs://{self.bucket_name}/{file_name}")

    def upload_parquet(self, data, file_name):
        blob = self.bucket.blob(file_name)

        # Upload the JSON string to the blob
        try:
            blob.upload_from_string(data, content_type='application/json; charset=utf-8')
            print(f"JSON file uploaded to GCS: gs://{self.bucket_name}/{file_name}")
        except:
            print(f"Error when uploading to gs://{self.bucket_name}/{file_name}")

    def download_json(self, blob):
        try:
            json_string = blob.download_as_text()
            print(f"JSON file downloaded from GCS: gs://{self.bucket_name}/{blob.name}")
        except:
            print(f"Error when downloading from GCS://{self.bucket_name}/{blob.name}")    


class BQHelper:

    def __init__(self, client=None, credentials_file=None):

        self.client = client
        if not self.client:
            self.credentials = service_account.Credentials.from_service_account_file(
                credentials_file or config.SERVICE_ACCOUNT, 
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
                )
            self.client = bigquery.Client(credentials=self.credentials, project=self.credentials.project_id)


    def get_table(self, table_id='project-id.dataset-id.table-id'):

        # TODO(developer): Set table_id to the ID of the model to fetch.
        # table_id = 'your-project.your_dataset.your_table'

        try:
            table = self.client.get_table(table_id)  # Make an API request.

            # View table properties
            print(
                "Got table '{}.{}.{}'.".format(table.project, table.dataset_id, table.table_id)
            )
            print("Table schema: {}".format(table.schema))
            print("Table description: {}".format(table.description))
            print("Table has {} rows".format(table.num_rows))

            return table.schema
        
        except Exception as e:
            return False

            # SELECT * EXCEPT(is_typed)
            # FROM mydataset.INFORMATION_SCHEMA.TABLES

    def execute(self, query):
        query_job = self.client.query(query)
        results = query_job.result()
        return results

    def select(self, query):
        try:
            data_frame = pandas_gbq.read_gbq(query,credentials=self.credentials,progress_bar_type=None)
        except Exception as e:
            print(e)
            return False
        return data_frame
    
    def load_gcs_to_bq(self, dataset_id, table_id, gcs_uri):
        # dataset_id = "my_dataset"  # Replace with your dataset
        # table_id = "my_table"  # Replace with your table

        # # Define the GCS folder URI (use wildcard * to load all JSON files in the folder)
        # gcs_uri = "gs://your-bucket-name/your-folder/*.json"  # Replace with your GCS path

        # Define job configuration
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,  # Automatically detects schema
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Overwrites table
        )

        # Load data from GCS folder to BigQuery table
        table_ref = self.client.dataset(dataset_id).table(table_id)
        load_job = self.client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)

        # Wait for the job to complete
        load_job.result()

        print(f"Loaded JSON files from {gcs_uri} into {dataset_id}.{table_id}")

    def copy_destination_table(self, source_dataset, source_table, destination_dataset, destination_table):
        # Reference to the source and new tables
        source_table_ref = self.client.dataset(source_dataset).table(source_table)
        new_table_ref = self.client.dataset(destination_dataset).table(destination_table)

        # Copy job configuration (schema only, no data)
        job_config = bigquery.CopyJobConfig(write_disposition="WRITE_EMPTY")

        # Copy table structure
        copy_job = self.client.copy_table(source_table_ref, new_table_ref, job_config=job_config)
        copy_job.result()  # Wait for the job to complete

        print(f"Table '{new_table_ref.table_id}' created successfully with schema from '{source_table_ref.table_id}'.")

    def get_columns(self, dataset_id, table_id):
        # Get the table schema
        table_ref = self.client.dataset(dataset_id).table(table_id)
        table = self.client.get_table(table_ref)

        # Extract column names
        column_names = [field.name for field in table.schema]

        return column_names

    
    # def bq_append(self, update_data, table_name, dataset_id, if_exists='append', project_id=config.PROJECT_ID):
    #     if update_data is None or update_data.shape[0] == 0:
    #         return False, "Empty DataFrame"
    #         # update_data=load_data.copy()

    #     table_id = f'{project_id}.{dataset_id}.{table_name}'

    #     for c in update_data.columns:
    #         type = str(update_data[c].dtypes)
    #         if type  == 'object':
    #             update_data[c] = update_data[c].astype("string")
    #         elif type  == 'datetime64[ns, UTC]':
    #             update_data[c] = update_data[c].dt.strftime(config.DWH_TIME_FORMAT)
    #             update_data[c] = pd.to_datetime(update_data[c],errors='coerce',format="%Y-%m-%d %H:%M:%S")
    #     try:
    #         pandas_gbq.to_gbq(update_data, destination_table=table_id,chunksize=50000,if_exists=if_exists,credentials=self.credentials, api_method="load_csv")
    #     except Exception as e:
    #         print(e)
    #         raise e