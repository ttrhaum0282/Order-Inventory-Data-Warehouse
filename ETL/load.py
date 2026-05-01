import pandas as pd
import pyodbc
import os
from datetime import datetime

INPUT_DIR = "transformed"

# Load vào Database
TARGET_SERVER   = "localhost"
TARGET_DATABASE = "Order_Inventory"

CONN_STR = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={TARGET_SERVER};"
    f"DATABASE={TARGET_DATABASE};"
    f"Trusted_Connection=yes;"
)


def get_connection():
    return pyodbc.connect(CONN_STR)


def load_csv(name: str) -> pd.DataFrame:
    path = os.path.join(INPUT_DIR, f"{name}.csv")
    return pd.read_csv(path, encoding="utf-8-sig")


# Update
def bulk_insert(conn, table: str, df: pd.DataFrame, chunk_size: int = 500):
    cursor = conn.cursor()

    # Lấy danh sách cột thực tế trong bảng DB
    cursor.execute("""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = ?
    """, table)
    db_cols = [row[0] for row in cursor.fetchall()]

    if not db_cols:
        print(f"Bảng '{table}' không tồn tại trong DB, bỏ qua.")
        return

    # Chỉ giữ lại cột có trong cả DataFrame lẫn DB
    valid_cols = [c for c in df.columns if c in db_cols]
    dropped    = [c for c in df.columns if c not in db_cols]
    if dropped:
        print(f"Bỏ qua cột không có trong DB: {dropped}")

    if not valid_cols:
        print(f"Không có cột nào khớp với DB cho bảng '{table}'.")
        print(f"Cột trong CSV : {list(df.columns)}")
        print(f"Cột trong DB  : {db_cols}")
        return

    df = df[valid_cols]

    # Kiểm tra bảng có cột identity không
    cursor.execute("""
        SELECT COUNT(*) FROM sys.columns
        WHERE object_id = OBJECT_ID(?) AND is_identity = 1
    """, table)
    has_identity = cursor.fetchone()[0] > 0

    cols         = ", ".join(df.columns)
    placeholders = ", ".join(["?"] * len(df.columns))
    sql          = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"

    if has_identity:
        cursor.execute(f"SET IDENTITY_INSERT {table} ON")

    total = 0
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i : i + chunk_size]
        cursor.executemany(sql, chunk.values.tolist())
        conn.commit()
        total += len(chunk)

    if has_identity:
        cursor.execute(f"SET IDENTITY_INSERT {table} OFF")
        conn.commit()

    print(f"  Loaded {total} rows → {table}")


def upsert_merge(conn, table: str, df: pd.DataFrame, key_col: str):
    cursor = conn.cursor()
    tmp    = f"#tmp_{table}"

    # Tạo bảng tạm
    cols         = ", ".join(df.columns)
    placeholders = ", ".join(["?"] * len(df.columns))
    col_defs     = ", ".join([f"{c} NVARCHAR(MAX)" for c in df.columns])

    cursor.execute(f"IF OBJECT_ID('tempdb..{tmp}') IS NOT NULL DROP TABLE {tmp}")
    cursor.execute(f"CREATE TABLE {tmp} ({col_defs})")
    cursor.executemany(f"INSERT INTO {tmp} ({cols}) VALUES ({placeholders})",
                       df.values.tolist())

    # Tạo SET clause (tất cả cột trừ key)
    non_keys  = [c for c in df.columns if c != key_col]
    set_clause = ", ".join([f"T.{c} = S.{c}" for c in non_keys])
    ins_cols   = ", ".join(df.columns)
    ins_vals   = ", ".join([f"S.{c}" for c in df.columns])

    merge_sql = f"""
        MERGE {table} AS T
        USING {tmp}   AS S ON T.{key_col} = S.{key_col}
        WHEN MATCHED     THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({ins_cols}) VALUES ({ins_vals});
    """
    cursor.execute(merge_sql)
    conn.commit()
    print(f"  Upserted → {table}")


# Thứ tự load phải đúng FK 
LOAD_ORDER = [
    ("Suppliers",    "SupplierID"),
    ("Customers",    "CustomerID"),
    ("Products",     "ProductID"),
    ("Inventory",    "InventoryID"),
    ("Orders",       "OrderID"),
    ("OrderDetails", "OrderDetailID"),
    ("SalesSummary", "OrderDetailID"),  # bảng fact, không có FK chặt
]


def truncate_all(conn):
    cursor = conn.cursor()
    for table in ["SalesSummary", "OrderDetails", "Orders", "Inventory", "Products", "Customers", "Suppliers"]:
        try:
            cursor.execute(f"DELETE FROM {table}")
            conn.commit()
            print(f"  Cleared {table}")
        except:
            conn.rollback()

def main():
    conn = get_connection()
    print("Connected!\n")
    
    truncate_all(conn)  # Xóa hết trước, đúng thứ tự FK
    
    for table, key in LOAD_ORDER:
        print(f"Loading {table}...")
        df = load_csv(table)
        for col in df.select_dtypes(include=["category"]).columns:
            df[col] = df[col].astype(str)
        bulk_insert(conn, table, df)  # bulk_insert giờ không cần DELETE nữa

    conn.close()


if __name__ == "__main__":
    main()