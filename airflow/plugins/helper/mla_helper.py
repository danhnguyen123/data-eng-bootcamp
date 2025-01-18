import requests

headers = {
  'accept': 'application/json'
}

BASE_URL = "https://api-mlastatistics.mla.com.au"

def report_5(from_date, to_date, indicator_id, page):

    url = f"{BASE_URL}/report/5?fromDate={from_date}&toDate={to_date}&indicatorID={indicator_id}&page={page}"

    response = requests.request("GET", url, headers=headers)

    # Get response code
    print(response.status_code)
    print(response.json())

    return response.json()

def report_6(from_date, to_date, indicator_id, saleyard_id, page):

    url = f"{BASE_URL}/report/6?fromDate={from_date}&toDate={to_date}&indicatorID={indicator_id}&saleyardID={saleyard_id}&page={page}"

    response = requests.request("GET", url, headers=headers)

    # Get response code
    print(response.status_code)
    print(response.json())

    return response.json()


