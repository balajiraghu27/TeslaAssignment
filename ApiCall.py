import requests
import json
from retry import retry


# This API Call retrieves the count of data for the specific period. Input parameter is Count URL
@retry(requests.exceptions.Timeout, tries=3, delay=3)
def getcount(counturl, starttme, endtime):
    params = {"format": "geojson", "starttime": starttme, "endtime": endtime}
    count = requests.get(counturl, params)
    return json.loads(count.text)["count"]


# This API Call retrieves data for the specific offset. Input parameters are Data URL and starting offset.
@retry(requests.exceptions.Timeout, tries=3, delay=3)
def getdata(datalink, offset, starttme, endtime, limit):
    paramss = {"format": "geojson", "starttime": starttme, "endtime": endtime, "offset": offset,
               "limit": limit, "orderby": "time-asc"}
    data = requests.get(datalink, params=paramss)
    data = json.loads(data.text)
    return data
