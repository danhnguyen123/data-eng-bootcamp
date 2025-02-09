

class Config:

    HUBSPOT_APP_TOKEN = ""
    HUBSPOT_BASE_URL = "https://api.hubapi.com"
    HUBSPOT_PAGE_SIZE = 100

    SERVICE_ACCOUNT = "/home/danhnguyen/workspace/data-engineering-bootcamp/airflow/secrets/service-account.json"

    BUCKET_NAME = "data-engineering-bootcamp"
    PROJECT_ID = "woven-plane-448115-r4"
    DATASET_STAGING = "staging"
    DATASET_WAREHOUSE = "warehouse"


config = Config()