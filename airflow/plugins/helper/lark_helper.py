import requests
from requests.exceptions import HTTPError, RequestException
import json

APP_ID = ""
APP_SECRET = ""
BASE_LARK_URL = "https://open.larksuite.com/open-apis"

class LarkHelper:


    def __init__(self):
        pass

    @property
    def headers(self):

        token = self.get_access_token()

        return {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}'
        }

    def get_access_token(self):

        headers = {
        'Content-Type': 'application/json'
        }

        url = f"{BASE_LARK_URL}/auth/v3/tenant_access_token/internal"

        payload = {
            "app_id": APP_ID,
            "app_secret": APP_SECRET
        }

        try:
            response = requests.request("POST", url, headers=headers, json=payload)

            if response.status_code == 200:
                response_json: dict = response.json()

                token = response_json.get("tenant_access_token")

                return token
            else:
                message = f"Error when getting tenant_access_token from Lark API, status_code: {response.status_code}, error: {response.text}"
                raise HTTPError(message)
                
        except Exception as e:
            raise e


    def get_group_information(self, chat_id):

        url = f"{BASE_LARK_URL}/im/v1/chats/{chat_id}"

        try:
            response = requests.request("GET", url, headers=self.headers)

            if response.status_code == 200:
                response_json: dict = response.json()
                return response_json
            else:
                message = f"Error when getting group {chat_id} from Lark API, status_code: {response.status_code}, error: {response.text}"
                raise HTTPError(message)
                
        except Exception as e:
            raise e