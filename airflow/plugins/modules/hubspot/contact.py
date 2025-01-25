from helper.hubspot_helper import HubspotHelper
import json


class HubspotContactETL:

    def __init__(self):
        self.hubspot = HubspotHelper()


    def extract(self):
        print("Start extact contact data from Hubspot API")

        properties = [
            "email",
            "phone",
        ]

        # First call
        page_index = 1

        results, after = self.hubspot.get_list_contacts(properties=properties)

        file_name = f"contacts.json"

        json_data = '\n'.join(json.dumps(data_dict, ensure_ascii=False) for data_dict in results)

        with open(file_name, "a") as json_file:
            json_file.write(json_data)

        while after:
            page_index = page_index + 1

            print(f"Getting data in page {page_index}")

            results, after = self.hubspot.get_list_contacts(properties=properties, after=after)

            json_data = '\n'.join(json.dumps(data_dict, ensure_ascii=False) for data_dict in results)

            with open(file_name, "a") as json_file:
                json_file.write(json_data)

        return "Success"



test = HubspotContactETL()
test.extract()



