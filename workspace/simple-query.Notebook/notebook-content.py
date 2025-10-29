# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "d617f2b6-1962-435f-a865-7652c09a1e9b",
# META       "workspaceId": "851f2673-aeb7-4b55-b663-82c63a597e07"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Cosmos DB in Fabric
# 
# ## Simple Query Samples
# 
# This sample notebook shows how to do simple queries for Cosmos DB in Fabric using the Sample Data you load from Cosmos Data Explorer. The sample dataset is a product catalog that also contains customer reviews, all stored in the same container. This co-locating of different entities is a best practice with Cosmos DB. If data is frequently accessed or updated at the same time, and if it can share the same partition key, then storing the different types of data is encouraged.
# 
# ### Features of this Notebook
# This notebook demonstrates the following concepts:
# 
# - How to authenticate to Cosmos DB in Fabric
# - How to get a database and container reference
# - How to write queries in Cosmos DB in Fabric
# 
# Requirements:
# - This sample utilizes the SampleData dataset in Cosmos DB in Fabric Data Explorer. Create a new Cosmos DB artifact, then on the Home screen after creating a new artifact, click SampleData to create the new SampleData container.


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
from rich.pretty import pprint
from typing import List, Dict, Any, Optional

from azure.cosmos.aio import CosmosClient
from azure.cosmos.exceptions import CosmosHttpResponseError

COSMOS_ENDPOINT = 'https://my-cosmos-endpoint.cosmos.fabric.microsoft.com:443/'
COSMOS_DATABASE_NAME = '{your-cosmos-artifact-name}'
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

# Initialize Cosmos DB client and container
COSMOS_CLIENT = CosmosClient(COSMOS_ENDPOINT, FabricTokenCredential())
DATABASE = COSMOS_CLIENT.get_database_client(COSMOS_DATABASE_NAME)
CONTAINER = DATABASE.get_container_client(COSMOS_CONTAINER_NAME)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

#Define function to search for all products by category name
async def search_products(categoryName: str, limit: int = 5) -> List[Dict[str, Any]]:

    try:
        # Use parameterized query
        query = """
            SELECT TOP @limit 
                c.categoryName,
                c.name, 
                c.description,
                c.currentPrice,
                c.inventory,
                c.priceHistory
            FROM c 
            WHERE 
                c.categoryName = @categoryName AND
                c.docType = @docType
            ORDER BY
                c.currentPrice DESC
        """

        parameters = [
            {"name": "@limit", "value": limit},
            {"name": "@docType", "value": "product"},
            {"name": "@categoryName", "value": categoryName}
        ]

        # Async query: gather results into a list
        products = [p async for p in CONTAINER.query_items(
            query=query,
            enable_cross_partition_query=False,
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
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Search for products in a category
products = await search_products(categoryName="Computers, Laptops", limit=5)

display(products) #For tabular output
pprint(products) #Json friendly output

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

#Define function to search for a single product and all its customer reviews
async def search_products_and_reviews(categoryName: str, productId: str) -> List[Dict[str, Any]]:

    try:
        # Use parameterized query
        query = """
            SELECT *
            FROM c 
            WHERE 
                c.categoryName = @categoryName AND
                c.productId = @productId
        """

        parameters = [
            {"name": "@categoryName", "value": categoryName},
            {"name": "@productId", "value": productId},
        ]

        # Async query: gather results into a list
        products = [p async for p in CONTAINER.query_items(
            query=query,
            parameters=parameters
        )]

        # Remove system properties (only when using SELECT *)
        for p in products:
            system_properties = [k for k in p.keys() if k.startswith('_')]
            for k in system_properties:
                p.pop(k)
        
        return products

    except CosmosHttpResponseError as e:
        logging.error(f"Cosmos DB query failed: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error in search_product_and_reviews: {e}")
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Search for a product and all reviews
products = await search_products_and_reviews(
    categoryName="Computers, Laptops", 
    productId="77be013f-4036-4311-9b5a-dab0c3d022be")

display(products) #For tabular output
pprint(products) #Json friendly output

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
