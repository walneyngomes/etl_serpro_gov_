import requests
from dagster import resource

@resource
def api_client():
    session = requests.Session()
    session.headers.update({
        "Accept": "application/json"
    })
    return session