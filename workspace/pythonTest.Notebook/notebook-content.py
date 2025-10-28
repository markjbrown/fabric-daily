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

! pip install azure-cosmos
! pip install openai
! pip install azure-core
! pip install azure-identity

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

from azure.identity import DefaultAzureCredential, get_bearer_token_provider
from azure.cosmos import CosmosClient
from azure.cosmos import PartitionKey


from openai import AzureOpenAI, AzureADTokenProvider
from openai.lib.azure import AzureOpenAI, AsyncAzureOpenAI, AzureADTokenProvider, AsyncAzureADTokenProvider

USER_CREDENTIAL = DefaultAzureCredential()

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

# Initialize the Azure AD token provider and Azure OpenAI client
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

# Initialize Cosmos DB client and container
COSMOS_CLIENT = CosmosClient(COSMOS_ENDPOINT, USER_CREDENTIAL)
DATABASE = client.get_database_client(COSMOS_DATABASE_NAME)
CONTAINER = DATABASE.get_container_client(COSMOS_CONTAINER_NAME)

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
def search_products(search_text, limit=5):

    embeddings = generate_embeddings("sample text")

    query = (
        "SELECT TOP {limit} *, VectorDistance(c.descriptionVector, {}) AS SimilarityScore "
        "FROM c "
        "WHERE c.docType = 'product' "
        "ORDER BY VectorDistance(c.descriptionVector, {})"
        .format(embeddings, embeddings).limit(limit)
    )

    products = list(CONTAINER.query_items(query=query, enable_cross_partition_query=True))

    # Remove /descriptionVector property from the the return list of products
    for product in products:
        if 'descriptionVector' in product:
            del product['descriptionVector']

    return products


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Search for products

products = search_products("wireless headphones", limit=5)

display(products)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
