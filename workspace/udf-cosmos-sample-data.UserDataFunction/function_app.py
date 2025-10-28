import fabric.functions as fn
udf = fn.UserDataFunctions()

import datetime
import logging
import uuid
from datetime import timezone
from azure.cosmos.aio import CosmosClient
from azure.cosmos.exceptions import CosmosHttpResponseError

COSMOS_URI = "https://c99cf5e7-df62-40c7-b347-65d9ca29aa27.zc9.daily-sql.cosmos.fabric.microsoft.com:443/"
COSMOS_DATABASE = "cdb-11"
COSMOS_CONTAINER = "SampleData"


cosmos_client = None
cosmos_client_expires_on = None


async def get_cosmos_client(fabric_item: fn.FabricItem, cosmos_db_uri: str) -> CosmosClient:
    
    try:
        global cosmos_client
        global cosmos_client_expires_on
        
        credential = fabric_item.get_access_token()
        if cosmos_client is not None and not has_cosmos_client_expired():
            return cosmos_client
        else:
            credential = fabric_item.get_access_token()
            cosmos_client_expires_on = credential.get_token().expires_on
            cosmos_client = CosmosClient(cosmos_db_uri, credential=credential)
            return cosmos_client
    except Exception as e:
        logging.error(f"Unexpected error in get_cosmos_client: {e}")
        raise

async def has_cosmos_client_expired():
    expiration_time = datetime.fromtimestamp(cosmos_client_expires_on, tz=timezone.utc)
    current_time = datetime.now(timezone.utc)
    return current_time >= expiration_time


#Define function to Cosmos query
@udf.generic_connection(argName="cosmosDb", audienceType="CosmosDB")
@udf.function()
async def expensive_category_products(cosmosDb: fn.FabricItem, category: str, limit: int) -> dict:

    try:

        print(1)
        # Initialize the Cosmos client with the URI and credential
        cosmos_client =  await get_cosmos_client(cosmosDb, COSMOS_URI)

        print(2)
        # Get the database and container
        database = cosmos_client.get_database_client(COSMOS_DATABASE)
        container = database.get_container_client(COSMOS_CONTAINER)

        print(3)
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
        products = [p async for p in container.query_items(
            query=query,
            parameters=parameters
        )]
        print(4)
        return products
        
    except CosmosHttpResponseError as e:
        logging.error(f"Cosmos DB query expensive_category_products failed: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error in expensive_category_products: {e}")
        raise


@udf.generic_connection(argName="cosmosDb", audienceType="CosmosDB")
@udf.function()
async def add_product(cosmosDb: fn.FabricItem) -> dict:
    
    # Initialize the Cosmos client with the URI and credential
    cosmos_client = await get_cosmos_client(cosmosDb, COSMOS_URI)

    # Get the database and container
    database = cosmos_client.get_database_client(COSMOS_DATABASE)
    container = database.get_container_client(COSMOS_CONTAINER)
    
    try:

        new_product = {
            "id": str(uuid.uuid4()),  # Generate a unique ID for the new product
            "name": "Super Awesome Computer",
            "price": 899.99,
            "category": "Electronics",
            "description": "This super awesome computer is fast.",
            "stock": 98,
            "countryOfOrigin": "Taiwan",
            "firstAvailable": "2025-09-16T18:00:00",
            "priceHistory": [ 899.99 ],
            "customerRatings": [],
            "rareProperty": False
        }

        # Insert the item into the container
        await container.create_item(body=new_product)

        print("New product added successfully.")

        return new_product

    except CosmosHttpResponseError:
        logging.error(f"add_product failed with: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error occurred in add_product: {e}")
        return None


@udf.generic_connection(argName="cosmosDb", audienceType="CosmosDB")
@udf.function()
async def get_single_product(cosmosDb: fn.FabricItem, category: str, itemId: str) -> dict:

    # Initialize the Cosmos client with the URI and credential
    cosmos_client = await get_cosmos_client(cosmosDb, COSMOS_URI)

    # Get the database and container
    database = cosmos_client.get_database_client(COSMOS_DATABASE)
    container = database.get_container_client(COSMOS_CONTAINER)

    try:
        # Perform async point read
        product = await container.read_item(
            item=itemId,
            partition_key=category
        )
        print(f"Retrieved product: {product}")
        return product
        
    except CosmosHttpResponseError.CosmosResourceNotFoundError:
        logging.error(f"Product with id '{itemId}' not found")
        return None
    except CosmosHttpResponseError as e:
        logging.error(f"HTTP error occurred: {e}")
        return None
