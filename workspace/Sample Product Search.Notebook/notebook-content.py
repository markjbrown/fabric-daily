# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

#Install packages

%pip install azure-cosmos==4.14.0b4
%pip install azure-core
%pip install azure-identity
%pip install aiohttp

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Imports and config values
import json
import os
import uuid
import datetime
import logging
import base64
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional


from azure.identity import DefaultAzureCredential, get_bearer_token_provider
from azure.core.credentials import AccessToken

from azure.cosmos.aio import CosmosClient
from azure.cosmos.exceptions import CosmosHttpResponseError


COSMOS_ENDPOINT = 'https://b205a15f-a454-4714-a74f-fb31e5c27568.zb2.daily-sql.cosmos.fabric.microsoft.com:443/'
COSMOS_DATABASE_NAME = 'cosmos-sample-database'
COSMOS_CONTAINER_NAME = 'SampleDataNew'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _decode_jwt_payload(jwt: str) -> dict:
    parts = jwt.split(".")
    if len(parts) < 2:
        raise ValueError("Not a valid JWT")
    pad = "=" * (-len(parts[1]) % 4)
    return json.loads(base64.urlsafe_b64decode(parts[1] + pad).decode("utf-8"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class CosmosDBTokenCredential :
    _audience = "https://cosmos.azure.com/" 
    _debug = False
    def __init__(self, access_token: str | None = None, expires_on: int | None = None):
        self._token = notebookutils.credentials.getToken(self._audience)
        payload = _decode_jwt_payload(self._token) 
        self._expires_on = int(payload["exp"])
        if self._debug: 
            print("credential init")
            print(f" expires : '{datetime.fromtimestamp(self._expires_on, tz=timezone.utc)}'")
            print(f"     now : '{datetime.fromtimestamp(int(time.time()), tz=timezone.utc)}'")
    
    def get_token(self, *scopes, **kwargs) -> AccessToken:
        now = int(time.time())
        if self._debug: 
            print("credential retreived")
            print(f" expires : '{datetime.fromtimestamp(self._expires_on, tz=timezone.utc)}'")
            print(f"     now : '{datetime.fromtimestamp(int(time.time()), tz=timezone.utc)}'")
        if (now >= self._expires_on - 60) :
            if self._debug: 
                print("credential expired")
            self._token = notebookutils.credentials.getToken(self._audience)
            payload = _decode_jwt_payload(self._token)
            self._expires_on = int(payload["exp"])
            if self._debug: 
                print("credential renewed")
                print(f" expires : '{datetime.fromtimestamp(self._expires_on, tz=timezone.utc)}'")
                print(f"     now : '{datetime.fromtimestamp(int(time.time()), tz=timezone.utc)}'")
        return AccessToken(self._token, self._expires_on)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Initialize Cosmos DB client and container
audience = "https://cosmos.azure.com/"
token = notebookutils.credentials.getToken(audience)
cosmos_cred = CosmosDBTokenCredential(token)

#COSMOS_CLIENT = CosmosClient(COSMOS_ENDPOINT, cosmos_cred)
COSMOS_CLIENT = CosmosClient(COSMOS_ENDPOINT, DefaultAzureCredential())
DATABASE = COSMOS_CLIENT.get_database_client(COSMOS_DATABASE_NAME)
CONTAINER = DATABASE.get_container_client(COSMOS_CONTAINER_NAME)

product = await CONTAINER.read_item(item='71abb6ae-6745-47cc-9834-c85855c43ff0', partition_key='Electronics')
print(product)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Define function to perform vector search
async def search_products(category: str, limit: int = 5) -> List[Dict[str, Any]]:

    try:

        # Use parameterized query
        query = """
            SELECT TOP @limit 
                c.category,
                c.name, 
                c.description,
                c.price,
                c.stock,
                c.priceHistory
            FROM c 
            WHERE 
                c.category = @category
            ORDER BY
                c.price DESC
        """

        parameters = [
            {"name": "@limit", "value": limit},
            {"name": "@category", "value": category}
        ]

        # Async query: gather results into a list
        products = [p async for p in CONTAINER.query_items(
            query=query,
            parameters=parameters
        )]
        
        return products

    except CosmosHttpResponseError as e:
        logging.error(f"Cosmos DB query failed: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error in search_products: {e}")
        raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Search for products

products = await search_products(category="Electronics", limit=5)

display(products)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
