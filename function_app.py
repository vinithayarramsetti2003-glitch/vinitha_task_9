# function_app.py
import os
import time
import json
import logging
import pyodbc
import azure.functions as func                       # <<< added
from azure.cosmos import CosmosClient, PartitionKey
from azure.storage.blob import BlobServiceClient
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from azure.cosmos.exceptions import CosmosHttpResponseError
from datetime import datetime
from typing import List, Tuple

# --- Function App instance required by Python v2 model ---
app = func.FunctionApp()                             # <<< added

# CONFIG — tune these
COSMOS_URI = os.environ['COSMOS_URI']
COSMOS_KEY = os.environ['COSMOS_KEY']
COSMOS_DB = os.environ.get('COSMOS_DB','productsdb')
COSMOS_CONTAINER = os.environ.get('COSMOS_CONTAINER','products')   # <<< fixed typo
SQL_CONN_STR = os.environ['SQL_CONN_STR']  # pyodbc connection string
BLOB_CONN = os.environ.get('BLOB_CONN')   # optional: for saving report
BLOB_CONTAINER = os.environ.get('BLOB_CONTAINER','migration-reports')

PAGE_SIZE = int(os.environ.get('PAGE_SIZE', '1000'))     # Cosmos page size
SQL_BATCH = int(os.environ.get('SQL_BATCH', '500'))     # SQL batch insert size
MAX_CONCURRENT = int(os.environ.get('MAX_CONCURRENT','4'))

# set up clients
cosmos_client = CosmosClient(COSMOS_URI, credential=COSMOS_KEY)
container = cosmos_client.get_database_client(COSMOS_DB).get_container_client(COSMOS_CONTAINER)

# SQL helper (pyodbc)
def get_sql_conn():
    conn = pyodbc.connect(SQL_CONN_STR, autocommit=False)
    # conn.fast_executemany = True  # sometimes useful, test in your environment
    return conn

# if blob reporting required
blob_client = BlobServiceClient.from_connection_string(BLOB_CONN) if BLOB_CONN else None

# Retry decorator for SQL/Cosmos transient ops
@retry(stop=stop_after_attempt(6), wait=wait_exponential(multiplier=1, min=1, max=60),
       retry=retry_if_exception_type(Exception))
def execute_sql_batch(conn, sql, params_list):
    cursor = conn.cursor()
    cursor.executemany(sql, params_list)
    conn.commit()
    cursor.close()

def transform_document(doc: dict) -> Tuple[Tuple, List[Tuple]]:
    """
    Map a Cosmos product doc to SQL row (product) and list of tag rows.
    Adjust field mapping as needed.
    """
    prod_id = doc.get('id') or doc.get('_id') or str(doc.get('productId'))
    name = doc.get('name')
    price = doc.get('price') if isinstance(doc.get('price'), (int, float)) else None
    category = doc.get('category')
    product_row = (prod_id, name, price, category)

    tags = doc.get('tags') or []
    tag_rows = []
    if isinstance(tags, list):
        for t in tags:
            tag_rows.append((prod_id, str(t)))
    return product_row, tag_rows

def insert_batches(product_rows, tag_rows, dry_run: bool = False):
    """
    Insert product_rows and tag_rows into SQL in batches.
    Both args are lists of tuples.
    If dry_run is True, skip actual DB writes.
    """
    if dry_run:
        logging.info(f"DRY RUN: would insert {len(product_rows)} products and {len(tag_rows)} tags")
        return

    conn = get_sql_conn()
    try:
        # Insert products
        prod_sql = ("MERGE INTO Products AS target "
                    "USING (VALUES (?, ?, ?, ?)) AS src(Id, Name, Price, Category) "
                    "ON target.Id = src.Id "
                    "WHEN MATCHED THEN UPDATE SET Name = src.Name, Price = src.Price, Category = src.Category "
                    "WHEN NOT MATCHED THEN INSERT (Id, Name, Price, Category) VALUES (src.Id, src.Name, src.Price, src.Category);")
        tag_sql = "INSERT INTO ProductTags (ProductId, Tag) VALUES (?, ?)"
        # break into batches
        for i in range(0, len(product_rows), SQL_BATCH):
            batch = product_rows[i:i+SQL_BATCH]
            execute_sql_batch(conn, prod_sql, batch)
        # tags
        for i in range(0, len(tag_rows), SQL_BATCH):
            batch = tag_rows[i:i+SQL_BATCH]
            execute_sql_batch(conn, tag_sql, batch)
    finally:
        conn.close()

def save_report(report: dict):
    if not blob_client:
        logging.info("No blob client configured; printing report.")
        logging.info(json.dumps(report, indent=2, default=str))
        return
    blob_name = f"cosmos-to-sql-report-{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.json"
    container_client = blob_client.get_container_client(BLOB_CONTAINER)
    try:
        container_client.create_container()
    except Exception:
        pass
    container_client.upload_blob(name=blob_name, data=json.dumps(report, default=str), overwrite=True)
    logging.info(f"Saved report to blob: {blob_name}")

# Main migration function
def migrate_all(start_ct: str = None, dry_run: bool = False):
    start_time = time.time()
    total = 0
    failures = []
    migrated_ids = []

    query = "SELECT * FROM c"
    iterator = container.query_items(query=query, enable_cross_partition_query=True, max_item_count=PAGE_SIZE).by_page(continuation_token=start_ct)


    continuation_token = None
    for page in iterator:
        docs = list(page)
        # capture continuation token for resumability
        try:
            continuation_token = iterator.continuation_token
        except Exception:
            continuation_token = None

        # Transform
        product_rows = []
        tag_rows = []
        for d in docs:
            try:
                prod_row, tags = transform_document(d)
                product_rows.append(prod_row)
                tag_rows.extend(tags)
                migrated_ids.append(prod_row[0])
            except Exception as e:
                failures.append({'doc': d.get('id','<no id>'), 'err': str(e)})

        # Insert into SQL in batches
        if product_rows or tag_rows:
            try:
                insert_batches(product_rows, tag_rows, dry_run=dry_run)
                total += len(product_rows)
            except Exception as e:
                logging.exception("SQL batch insert failed")
                failures.append({'continuation_token': continuation_token, 'err': str(e)})
                # Decide whether to break or continue; here we break to allow human fix
                break

    duration = time.time() - start_time
    report = {
        'start_time': datetime.utcnow().isoformat() + 'Z',
        'duration_seconds': int(duration),
        'total_products': total,
        'failures': failures,
        'last_continuation_token': continuation_token,
        'migrated_ids_sample': migrated_ids[:20]
    }
    save_report(report)
    return report

# -------------------------
# HTTP trigger for Azure Functions (v2 model)
# -------------------------
def _get_request_values(req: func.HttpRequest):
    params = req.params or {}
    try:
        body = req.get_json(silent=True) or {}
    except Exception:
        body = {}
    continuation_token = params.get('continuation_token') or body.get('continuation_token')
    dry_run = params.get('dry_run') or body.get('dry_run') or False
    if isinstance(dry_run, str):
        dry_run = dry_run.lower() == "true"
    return continuation_token, dry_run

@app.route(route="migrate", methods=["GET", "POST"])   # registers /api/migrate
def http_migrate(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Migration HTTP called")
    try:
        start_ct, dry_run = _get_request_values(req)
        report = migrate_all(start_ct=start_ct, dry_run=dry_run)
        return func.HttpResponse(json.dumps(report, default=str), status_code=200, mimetype="application/json")
    except Exception as e:
        logging.exception("Migration failed")
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")
