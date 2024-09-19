import requests


# Schema Registry URL
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"

# Fetch and parse schema from Schema Registry
def get_schema(subject, version=1):
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/{version}"
    response = requests.get(url)
    response.raise_for_status()
    schema = response.json()["schema"]
    print(schema)
    return schema

