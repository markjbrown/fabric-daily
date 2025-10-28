# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

#Install packages

%pip install azure-cosmos
#%pip install openai
%pip install azure-core
%pip install azure-identity
%pip install aiohttp

# Install the fixed version of packages.
%pip install -q --force-reinstall openai==1.30 2>/dev/null

# Install the latest version of SynapseML-core.
%pip install -q --force-reinstall https://mmlspark.blob.core.windows.net/pip/1.0.12-spark3.5/synapseml_core-1.0.12.dev1-py2.py3-none-any.whl 2>/dev/null

# Install SynapseML-Internal .whl with the AI functions library from blob storage:
%pip install -q --force-reinstall https://mmlspark.blob.core.windows.net/pip/1.0.12.2-spark3.5/synapseml_internal-1.0.12.2.dev1-py2.py3-none-any.whl 2>/dev/null

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

notebookutils.help()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Required imports
import synapse.ml.aifunc as aifunc
import pandas as pd
import openai

# Optional import for progress bars
from tqdm.auto import tqdm
tqdm.pandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

aifunc.DEFAULT_API_VERSION = "2024-12-01-preview"
aifunc.DEFAULT_EMBEDDING_MODEL = "text-embedding-3-large"



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
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

from azure.cosmos import CosmosClient
from azure.cosmos import PartitionKey
from azure.cosmos.exceptions import CosmosHttpResponseError


#from openai import AzureOpenAI, AzureADTokenProvider
#from openai.lib.azure import AzureOpenAI, AsyncAzureOpenAI, AzureADTokenProvider, AsyncAzureADTokenProvider

# USER_CREDENTIAL = DefaultAzureCredential()

OPENAI_ENDPOINT = 'https://byocdemo-openai.openai.azure.com/'
OPENAI_API_VERSION = "2024-12-01-preview"
OPENAI_EMBEDDING_DIMENSIONS = 512
OPENAI_EMBEDDING_MODEL_DEPLOYMENT = 'text-3-large'
OPENAI_AUTH_SCOPE = "https://cognitiveservices.azure.com/.default"

COSMOS_ENDPOINT = 'https://c99cf5e7-df62-40c7-b347-65d9ca29aa27.zc9.daily-sql.cosmos.fabric.microsoft.com:443/'
COSMOS_DATABASE_NAME = 'cosmos-sample-database-1'
COSMOS_CONTAINER_NAME = 'SampleData'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
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
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# TokenCredential Class
class TokenCredential:
    def __init__(self, access_token: str | None = None, expires_on: int | None = None):
        self._token = access_token
        if expires_on is None:
            payload = _decode_jwt_payload(access_token)
            expires_on = int(payload["exp"])
        self._expires_on = int(expires_on)

    def get_token(self, *scopes, **kwargs) -> AccessToken:
        now = int(time.time())
        if now >= self._expires_on - 60:
            raise RuntimeError("The provided access token is expired or about to expire.")
        return AccessToken(self._token, self._expires_on)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

class CosmosDBTokenCredential :
    _audience = "https://cosmos.azure.com/" 
    _debug = True
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
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Initialize the Azure AD token provider and Azure OpenAI client
USER_CREDENTIAL = DefaultAzureCredential()
OPENAI_AUTH_SCOPE = "https://cognitiveservices.azure.com/.default"
TOKEN_PROVIDER: AzureADTokenProvider = get_bearer_token_provider(USER_CREDENTIAL, OPENAI_AUTH_SCOPE)

OPENAI_CLIENT = AzureOpenAI(
    api_version=OPENAI_API_VERSION,
    azure_endpoint=OPENAI_ENDPOINT,
    azure_ad_token_provider=TOKEN_PROVIDER
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Initialize Azure OpenAI client using fabric notebook utils
audience = "https://cognitiveservices.azure.com/.default"
token = notebookutils.credentials.getToken(audience)

#TOKEN_PROVIDER = TokenCredential(token)
    
OPENAI_CLIENT = AzureOpenAI(
    api_version=OPENAI_API_VERSION,
    azure_endpoint=OPENAI_ENDPOINT,
    azure_ad_token_provider=TOKEN_PROVIDER
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Initialize Cosmos DB client and container
audience = "https://cosmos.azure.com/"
token = notebookutils.credentials.getToken(audience)
print (token)
cosmos_cred = TokenCredential(token)
#cosmos_cred = CosmosDBTokenCredential(token)

COSMOS_CLIENT = CosmosClient(COSMOS_ENDPOINT, cosmos_cred)
DATABASE = COSMOS_CLIENT.get_database_client(COSMOS_DATABASE_NAME)
CONTAINER = DATABASE.get_container_client(COSMOS_CONTAINER_NAME)

blah = CONTAINER.read_item(item='71abb6ae-6745-47cc-9834-c85855c43ff0', partition_key='Electronics')

print(blah)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Define function to generate embeddings for vector search
def generate_embeddings(text):
    
    response = OPENAI_CLIENT.embeddings.create(
        input = text, 
        dimensions = OPENAI_EMBEDDING_DIMENSIONS,
        model = OPENAI_EMBEDDING_MODEL_DEPLOYMENT)
    
    embeddings = response.model_dump()
    return embeddings['data'][0]['embedding']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

#Define function to perform vector search
def search_products(search_text: str, limit: int = 5) -> List[Dict[str, Any]]:

    try:

        embeddings = generate_embeddings(search_text.strip())

        # Use parameterized query to avoid SQL injection and duplication
        query = """
            SELECT TOP @limit 
                c.name, 
                c.description,
                c.price,
                c.category,
                VectorDistance(c.descriptionVector, @embeddings) AS SimilarityScore
            FROM c 
            WHERE c.docType = @docType
            ORDER BY VectorDistance(c.descriptionVector, @embeddings)
        """

        parameters = [
            {"name": "@limit", "value": limit},
            {"name": "@embeddings", "value": embeddings},
            {"name": "@docType", "value": "product"}
        ]

        products = list(CONTAINER.query_items(
            query=query,
            parameters=parameters
        ))

        # Remove /descriptionVector property from the the return list of products
        for product in products:
            if 'descriptionVector' in product:
                del product['descriptionVector']

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
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Search for products

products = search_products(search_text="computer", limit=5)

display(products)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
