import requests
from config import config
from requests.exceptions import HTTPError

class HubspotHelper:

    # Initialize class
    def __init__(self):

        self.session = self.request_session()

    def request_session(self):
        '''
        Create a session request
        '''
        session = requests.Session()
        return session

        
    @property
    def headers(self):

        return {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {config.HUBSPOT_APP_TOKEN}'
        }

    def get_list_contacts(self, limit=None, after=None, properties: list = None):
        
        url = f"{config.HUBSPOT_BASE_URL}/crm/v3/objects/contacts"

        params = {
            "limit": limit if limit else config.HUBSPOT_PAGE_SIZE,
        }

        if after:
            params.update({"after": after})
        if properties:
            properties = ','.join(i for i in properties)
            params.update({"properties": properties})


        try:
            response = self.session.get(url=url, params=params, headers=self.headers)

            if response.status_code == 200:
                response_json: dict = response.json()

                results = response_json.get("results")
                after = response_json.get("paging", {}).get("next", {}).get("after")

                return results, after
            
            else:
                message = f"Error when getting contacr from Hubspot API, status_code: {response.status_code}, error: {response.text}"
                raise HTTPError(message)
                
        except Exception as e:
            raise e