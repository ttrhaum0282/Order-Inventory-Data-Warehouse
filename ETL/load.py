import pandas as pd
import os
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

# Cấu hình để load file CSV trên Google Cloud
PROJECT_ID   = "ttcs-project"
DATASET_ID   = "order_inventory"
KEY_PATH     = "google_cloud_key.json"          
INPUT_DIR    = "transformed"                     # output của transform.py

def get_bq_client() -> bigquery.Client:
    creds = service_account.Credentials.from_service_account_file(
        KEY_PATH,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(project=PROJECT_ID, credentials=creds)


def load_csv(table: str) -> pd.DataFrame:
    path = os.path.join(INPUT_DIR, f"{table}.csv")
    return pd.read_csv(path, encoding="utf-8-sig")


# Schema BigQuery cho từng bảng
# Chỉ khai báo các cột cần ép kiểu đặc biệt

SCHEMAS: dict[str, list[bigquery.SchemaField]] = {
    "Suppliers": [
        bigquery.SchemaField("SupplierID",   "INTEGER"),
        bigquery.SchemaField("SupplierName", "STRING"),
        bigquery.SchemaField("Phone",        "STRING"),
        bigquery.SchemaField("Address",      "STRING"),
    ],
    "Customers": [
        bigquery.SchemaField("CustomerID",   "INTEGER"),
        bigquery.SchemaField("CustomerName", "STRING"),
        bigquery.SchemaField("Phone",        "STRING"),
        bigquery.SchemaField("Email",        "STRING"),
        bigquery.SchemaField("Address",      "STRING"),
    ],
    "Products": [
        bigquery.SchemaField("ProductID",    "INTEGER"),
        bigquery.SchemaField("ProductName",  "STRING"),
        bigquery.SchemaField("SupplierID",   "INTEGER"),
        bigquery.SchemaField("Price",        "STRING"),
        bigquery.SchemaField("Category",     "STRING"),
        bigquery.SchemaField("PriceSegment", "STRING"),
    ],
    "Inventory": [
        bigquery.SchemaField("InventoryID",      "INTEGER"),
        bigquery.SchemaField("ProductID",        "INTEGER"),
        bigquery.SchemaField("QuantityInStock",  "INTEGER"),
        bigquery.SchemaField("LastUpdated",      "TIMESTAMP"),
        bigquery.SchemaField("StockStatus",      "STRING"),
    ],
    "Orders": [
        bigquery.SchemaField("OrderID",      "INTEGER"),
        bigquery.SchemaField("CustomerID",   "INTEGER"),
        bigquery.SchemaField("OrderDate",    "DATE"),
        bigquery.SchemaField("TotalAmount",  "INTEGER"),
        bigquery.SchemaField("OrderYear",    "INTEGER"),
        bigquery.SchemaField("OrderMonth",   "INTEGER"),
        bigquery.SchemaField("OrderQuarter", "INTEGER"),
        bigquery.SchemaField("OrderWeekday", "STRING"),
    ],
    "OrderDetails": [
        bigquery.SchemaField("OrderDetailID", "INTEGER"),
        bigquery.SchemaField("OrderID",       "INTEGER"),
        bigquery.SchemaField("ProductID",     "INTEGER"),
        bigquery.SchemaField("Quantity",      "INTEGER"),
        bigquery.SchemaField("Price",         "STRING"),
        bigquery.SchemaField("UnitPrice",     "STRING"),
    ],
    "SalesSummary": [
        bigquery.SchemaField("OrderDetailID", "INTEGER"),
        bigquery.SchemaField("OrderID",       "INTEGER"),
        bigquery.SchemaField("ProductID",     "INTEGER"),
        bigquery.SchemaField("Quantity",      "INTEGER"),
        bigquery.SchemaField("Price",         "STRING"),
        bigquery.SchemaField("UnitPrice",     "STRING"),
        bigquery.SchemaField("CustomerID",    "INTEGER"),
        bigquery.SchemaField("OrderDate",     "DATE"),
        bigquery.SchemaField("OrderYear",     "INTEGER"),
        bigquery.SchemaField("OrderMonth",    "INTEGER"),
        bigquery.SchemaField("OrderQuarter",  "INTEGER"),
        bigquery.SchemaField("ProductName",   "STRING"),
        bigquery.SchemaField("Category",      "STRING"),
        bigquery.SchemaField("CustomerName",  "STRING"),
    ],
}


# Chuẩn bị DataFrame trước khi push (ép kiểu khớp với schema)
def prepare_df(df: pd.DataFrame, table: str) -> pd.DataFrame:
    df = df.copy()

    # STRING columns bị pandas đọc nhầm sang số
    for col in ["Email", "Address", "SupplierName", "CustomerName"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Phone: pad số 0 đầu bị mất khi pandas cast sang int64
    if "Phone" in df.columns:
        df["Phone"] = df["Phone"].astype(str).str.strip().str.zfill(10)

    # DATE columns
    date_cols = {"Orders": ["OrderDate"], "SalesSummary": ["OrderDate"]}
    for col in date_cols.get(table, []):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    # TIMESTAMP columns
    if table == "Inventory" and "LastUpdated" in df.columns:
        df["LastUpdated"] = pd.to_datetime(df["LastUpdated"], errors="coerce")

    # Categorical → string (PriceSegment, StockStatus)
    for col in df.select_dtypes(include="category").columns:
        df[col] = df[col].astype(str)

    return df


def push_to_bq(client: bigquery.Client, df: pd.DataFrame, table: str):
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table}"
    schema    = SCHEMAS.get(table, [])

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # overwrite mỗi lần chạy
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # chờ job xong

    loaded = client.get_table(table_ref).num_rows
    print(f"{table:<15} {loaded:>6} rows → {table_ref}")


def main():
    print(f"\n[{datetime.now():%H:%M:%S}] Starting load to BigQuery...\n")

    client = get_bq_client()
    print(f"  Connected: {PROJECT_ID}.{DATASET_ID}\n")

    tables = ["Suppliers", "Customers", "Products", "Inventory",
              "Orders", "OrderDetails", "SalesSummary"]

    for table in tables:
        try:
            df = load_csv(table)
            df = prepare_df(df, table)
            push_to_bq(client, df, table)
        except FileNotFoundError:
            print(f"{table}: file not found — chạy transform.py trước!")
        except Exception as e:
            print(f"{table}: {e}")

    print(f"\n[{datetime.now():%H:%M:%S}] Load done.")


if __name__ == "__main__":
    main()