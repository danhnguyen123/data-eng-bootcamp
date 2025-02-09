from helper.hubspot_helper import HubspotHelper
import json
from helper.gcp_helper import GCSHelper, BQHelper
from config import config

gcs = GCSHelper(bucket_name="data-engineering-bootcamp")
execution_date = "2025-02-09"
bq = BQHelper()

class HubspotContactETL:

    def __init__(self, gcs: GCSHelper, bq: BQHelper, execution_date: str, table_id: str):
        self.hubspot = HubspotHelper()
        self.gcs = gcs
        self.bq = bq
        self.execution_date = execution_date
        self.project_id = config.PROJECT_ID
        self.dataset_staging = config.DATASET_STAGING
        self.dataset_warehouse = config.DATASET_WAREHOUSE
        self.table_id = table_id


    def extract(self):
        print("Start extact contact data from Hubspot API")

        properties = [
            "email",
            "phone",
        ]

        # First call
        page_index = 1

        print(f"Getting data in page {page_index}")

        results, after = self.hubspot.get_list_contacts(properties=properties)

        file_name = f"hubspot/contact/{self.execution_date}/contacts_{page_index}.json"

        json_data = []
        for data_dict in results:
            data_dict["email"] = data_dict.get("properties").get("email")
            data_dict["phone"] = data_dict.get("properties").get("phone")
            data_dict["created_at"] = data_dict.get("createdAt")
            data_dict["updated_at"] = data_dict.get("properties").get("lastmodifieddate")
            data_dict.pop("properties")
            data_dict.pop("archived")
            data_dict.pop("createdAt")
            data_dict.pop("updatedAt")
            json_data.append(data_dict)

        json_data = '\n'.join(json.dumps(data_dict, ensure_ascii=False) for data_dict in results)

        self.gcs.upload_json(json_data, file_name)

        # json_data = '\n'.join(json.dumps(data_dict, ensure_ascii=False) for data_dict in results)

        # with open(file_name, "a") as json_file:
        #     json_file.write(json_data)

        while after:
            # Next call
            page_index = page_index + 1

            print(f"Getting data in page {page_index}")

            results, after = self.hubspot.get_list_contacts(properties=properties, after=after)

            file_name = f"hubspot/contact/{self.execution_date}/contacts_{page_index}.json"

            json_data = []
            for data_dict in results:
                data_dict["email"] = data_dict.get("properties").get("email")
                data_dict["phone"] = data_dict.get("properties").get("phone")
                data_dict["created_at"] = data_dict.get("createdAt")
                data_dict["updated_at"] = data_dict.get("properties").get("lastmodifieddate")
                data_dict.pop("properties")
                data_dict.pop("archived")
                data_dict.pop("createdAt")
                data_dict.pop("updatedAt")
                json_data.append(data_dict)

            json_data = '\n'.join(json.dumps(data_dict, ensure_ascii=False) for data_dict in results)

            self.gcs.upload_json(json_data, file_name)


            # json_data = '\n'.join(json.dumps(data_dict, ensure_ascii=False) for data_dict in results)

            # with open(file_name, "a") as json_file:
            #     json_file.write(json_data)

        return "Success"

    def transform(self):
        gcs_uri = f"gs://{config.BUCKET_NAME}/hubspot/contact/{self.execution_date}/*.json"
        # Load to staging table
        self.bq.load_gcs_to_bq(dataset_id=self.dataset_staging, table_id=self.table_id, gcs_uri=gcs_uri)
        # Load to warehouse table if it not exist
        self.bq.copy_destination_table(source_dataset=self.dataset_staging, source_table=self.table_id,
                                       destination_dataset=self.dataset_warehouse, destination_table=self.table_id)

    def load(self):
        identifier_cols = ['id']

        table_cols = self.bq.get_columns(dataset_id=self.dataset_staging, table_id=self.table_id)

        on_clause = " and ".join([f"target.{col}=source.{col}" for col in table_cols])

        update_set_clause = ", ".join([f"target.{col} = source.{col}" for col in table_cols if col not in identifier_cols])
        insert_values_clause = ", ".join([f"source.{col}" for col in table_cols])

        merge_query = f"""
        MERGE `{self.project_id}.{self.dataset_warehouse}.{self.table_id}` AS target
        USING `{self.project_id}.{self.dataset_staging}.{self.table_id}` AS source
        ON {on_clause}
        WHEN MATCHED THEN
            UPDATE SET {update_set_clause}
        WHEN NOT MATCHED THEN 
            INSERT ({", ".join(table_cols)}) 
            VALUES ({insert_values_clause}) 
        """

        print(merge_query)
        results = self.bq.execute(query=merge_query)
        print(f"Job ID: {results.job_id}")

        return "Success"  






test = HubspotContactETL(gcs=gcs, bq=bq, execution_date=execution_date, table_id="contacts")
# test.transform()
test.load()



# bq.get_table("woven-plane-448115-r4.staging.contacts")