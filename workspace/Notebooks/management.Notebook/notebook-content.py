# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Cosmos DB in Fabric
# 
# ## Management Operations
# 
# This sample notebook shows how to common management tasks with Cosmos DB in Fabric containers.
# 
# ### Features of this Notebook
# This notebook demonstrates the following concepts:
# 
# - How to define indexing and vector policies for a container
# - How to create a new container with autoscale throughput
# - How to read and update container throughput
# 
# Requirements:
# - This sample requires creating a Cosmos DB artifact in your Fabric Workspace.


# CELL ********************

#Install packages
%pip install azure-cosmos

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

#Imports and config values
import logging
import requests
import asyncio
from typing import List, Dict, Any, Optional

from azure.cosmos.aio import CosmosClient
from azure.cosmos import PartitionKey, ThroughputProperties
from azure.cosmos.exceptions import CosmosHttpResponseError, CosmosResourceNotFoundError

COSMOS_ENDPOINT = 'https://e7ca4ac4-a09e-436e-ae9b-1030f2bfdd80.ze7.daily-sql.cosmos.fabric.microsoft.com:443/'
COSMOS_DATABASE_NAME = 'CosmosSampleDatabase'
COSMOS_CONTAINER_NAME = 'SampleData'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Custom TokenCredential implementation for Fabric authentication
%pip install azure-core
from azure.core.credentials import TokenCredential, AccessToken
import base64
import json
import notebookutils
from datetime import datetime, timezone

class FabricTokenCredential(TokenCredential):

    def get_token(self, *scopes: str, claims: Optional[str] = None, tenant_id: Optional[str] = None,
                  enable_cae: bool = False, **kwargs: Any) -> AccessToken:
        access_token = notebookutils.credentials.getToken("https://cosmos.azure.com/")
        parts = access_token.split(".")
        if len(parts) < 2:
            raise ValueError("Invalid JWT format")
        payload_b64 = parts[1]
        # Fix padding
        padding = (-len(payload_b64)) % 4
        if padding:
            payload_b64 += "=" * padding
        payload_json = base64.urlsafe_b64decode(payload_b64.encode("utf-8")).decode("utf-8")
        payload = json.loads(payload_json)
        exp = payload.get("exp")
        if exp is None:
            raise ValueError("exp claim missing in token")
        return AccessToken(token=access_token, expires_on=exp) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Initialize Cosmos DB client and database
COSMOS_CLIENT = CosmosClient(COSMOS_ENDPOINT, FabricTokenCredential())
DATABASE = COSMOS_CLIENT.get_database_client(COSMOS_DATABASE_NAME)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Creates and configure a container

async def create_container():

    # Define the indexing policy for the container
    indexing_policy = {
        "includedPaths": [
            {
                "path": "/*"
            }
        ],
        "excludedPaths": [
            {
                "path": "/\"_etag\"/?"
            }
        ]
    }

    # Containers in Fabric portal are created with 5000 RU/s by default
    # You can create containers via SDK with minimum of 1000 RU/s
    # Create the sample product container with 1000 RU/s
    CONTAINER = await DATABASE.create_container_if_not_exists(
        id=COSMOS_CONTAINER_NAME,
        partition_key=PartitionKey(path='/categoryName', kind='Hash'),
        indexing_policy=indexing_policy,
        offer_throughput=ThroughputProperties(auto_scale_max_throughput=5000))

    print(f"Container created")
    return CONTAINER


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Call the create_container() function
CONTAINER = await create_container()

# Load the product data.
url = "https://raw.githubusercontent.com/AzureCosmosDB/cosmos-fabric-samples/refs/heads/main/datasets/fabricSampleData.json"
data = requests.get(url).json()

await load_data(CONTAINER, data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Creates and configure a container for vector indexing

async def create_vector_container_1536():
    
    # Define the vector policy for the container
    vector_embedding_policy = {
        "vectorEmbeddings": [
            {
                "path":"/vectors",
                "dataType":"float32",
                "distanceFunction":"cosine",
                "dimensions":1536
            }
        ]
    }

    # Define the indexing policy for the container
    indexing_policy = {
        "includedPaths": [
            {
                "path": "/*"
            }
        ],
        "excludedPaths": [
            {
                "path": "/vectors/*"
            },
            {
                "path": "/\"_etag\"/?"
            }
        ],
        "vectorIndexes": [
            {
                "path": "/vectors",
                "type": "quantizedFlat"
            }
        ]
    }

    # Containers in Fabric portal are created with 5000 RU/s by default
    # You can create containers via SDK with minimum of 1000 RU/s
    # Create the vectorized sample product container with 1000 RU/s
    CONTAINER = await DATABASE.create_container_if_not_exists(
        id=COSMOS_CONTAINER_NAME,
        partition_key=PartitionKey(path='/categoryName', kind='Hash'),
        indexing_policy=indexing_policy,
        vector_embedding_policy=vector_embedding_policy,
        offer_throughput=ThroughputProperties(auto_scale_max_throughput=5000))

    print(f"Container created")
    return CONTAINER


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Call the create_container() function
CONTAINER = await create_vector_container_1536()

# Load the vectorized product data. Vectors generated using text-embedding-ada-002 with 1536 dimensions
url = "https://raw.githubusercontent.com/AzureCosmosDB/cosmos-fabric-samples/refs/heads/main/datasets/fabricSampleDataVectors-ada-002-1536.json"
data = requests.get(url).json()

await load_data(CONTAINER, data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Creates and configure a container for vector indexing with 512 dimensions
async def create_vector_text3_container():
    
    # Define the vector policy for the container
    vector_embedding_policy = {
        "vectorEmbeddings": [
            {
                "path":"/vectors",
                "dataType":"float32",
                "distanceFunction":"cosine",
                "dimensions":512
            }
        ]
    }

    # Define the indexing policy for the container
    indexing_policy = {
        "includedPaths": [
            {
                "path": "/*"
            }
        ],
        "excludedPaths": [
            {
                "path": "/vectors/*"
            },
            {
                "path": "/\"_etag\"/?"
            }
        ],
        "vectorIndexes": [
            {
                "path": "/vectors",
                "type": "quantizedFlat"
            }
        ]
    }

    # Containers in Fabric portal are created with 5000 RU/s by default
    # You can create containers via SDK with minimum of 1000 RU/s
    # Create the vectorized sample product container with 1000 RU/s
    CONTAINER = await DATABASE.create_container_if_not_exists(
        id=COSMOS_CONTAINER_NAME,
        partition_key=PartitionKey(path='/categoryName', kind='Hash'),
        indexing_policy=indexing_policy,
        vector_embedding_policy=vector_embedding_policy,
        offer_throughput=ThroughputProperties(auto_scale_max_throughput=5000))

    print(f"Container created")
    return CONTAINER


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

CONTAINER = await create_vector_text3_container()

# Load the vectorized product data. Vectors generated using text-embedding-3-large with 512 dimensions
url = "https://raw.githubusercontent.com/AzureCosmosDB/cosmos-fabric-samples/refs/heads/main/datasets/fabricSampleDataVectors-3-large-512.json"
data = requests.get(url).json()

await load_data(CONTAINER, data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

async def load_data(CONTAINER, data):

    i = 0
    # Insert the data into the container with retry logic for 429 errors
    for item in data:
        retry_count = 0
        max_retries = 5
        
        while retry_count <= max_retries:
            try:
                await CONTAINER.create_item(item)
                i += 1
                break  # Success, move to next item
                
            except CosmosHttpResponseError as e:
                if e.status_code == 429:  # Rate limited
                    retry_count += 1
                    if retry_count > max_retries:
                        print(f"Max retries exceeded for item {i}. Skipping.")
                        break
                    
                    # Extract retry-after-ms from response headers
                    retry_after_ms = e.headers.get('x-ms-retry-after-ms', '1000')
                    retry_after_seconds = int(retry_after_ms) / 1000.0
                    
                    print(f"Rate limited (429). Retrying item {i} after {retry_after_seconds} seconds (attempt {retry_count}/{max_retries})")
                    await asyncio.sleep(retry_after_seconds)
                else:
                    # Other errors, re-raise
                    print(f"Error loading item {i}: {e}")
                    raise
            except Exception as e:
                print(f"Unexpected error loading item {i}: {e}")
                raise

    print(f"{i} Products loaded successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Get the current throughput on the created container and increase it to 1000 RU/s

throughput_properties = await CONTAINER.get_throughput()
autoscale_throughput = throughput_properties.auto_scale_max_throughput

print(print(f"Autoscale throughput: {autoscale_throughput}"))

new_throughput = autoscale_throughput + 1000

await CONTAINER.replace_throughput(ThroughputProperties(auto_scale_max_throughput=new_throughput))

# Verify the updated throughput
updated_throughput_properties = await CONTAINER.get_throughput()
print(f"Verified updated autoscale throughput: {updated_throughput_properties.auto_scale_max_throughput}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
