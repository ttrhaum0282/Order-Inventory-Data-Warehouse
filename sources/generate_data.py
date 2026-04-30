import pyodbc
import random
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

fake = Faker('vi_VN')

SERVER = 'localhost'
DATABASE = 'Order_Inventory'

CONN_STR = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"Trusted_Connection=yes;"
)

N_SUPPLIERS = 100
N_CUSTOMERS = 500
N_PRODUCTS = 300
N_ORDERS = 2000
N_ORDER_DETAILS = 5000


def get_connection():
    return pyodbc.connect(CONN_STR)

def truncate_all(conn):
    cursor = conn.cursor()
    # Xóa theo thứ tự FK
    deletes = [
        "DELETE FROM OrderDetails",
        "DELETE FROM Orders",
        "DELETE FROM Inventory",
        "DELETE FROM Products",
        "DELETE FROM Customers",
        "DELETE FROM Suppliers",
    ]
    # Reset IDENTITY về 1
    reseeds = [
        "DBCC CHECKIDENT ('OrderDetails', RESEED, 0)",
        "DBCC CHECKIDENT ('Orders', RESEED, 0)",
        "DBCC CHECKIDENT ('Inventory', RESEED, 0)",
        "DBCC CHECKIDENT ('Products', RESEED, 0)",
        "DBCC CHECKIDENT ('Customers', RESEED, 0)",
        "DBCC CHECKIDENT ('Suppliers', RESEED, 0)",
    ]
    for sql in deletes + reseeds:
        cursor.execute(sql)
    conn.commit()
    print("Truncated & reset identity all tables\n")

def insert_df(conn, table: str, df: pd.DataFrame):
    cursor = conn.cursor()
    cols = ", ".join(df.columns)
    placeholders = ", ".join(["?"] * len(df.columns))
    sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
    cursor.executemany(sql, df.values.tolist())
    conn.commit()
    print(f"Inserted {len(df)} rows -> {table}")


# GENERATING DATA
# 1.Suppliers
def gen_suppliers(n: int) -> pd.DataFrame:
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "SupplierID": i,
            "SupplierName": fake.company(),
            "Phone": fake.phone_number()[:20],
            "Address": fake.address().replace("\n", ", ")[:200],
        })
    return pd.DataFrame(rows)


# 2. Customers
def gen_customers(n: int) -> pd.DataFrame:
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "CustomerID": i,
            "CustomerName": fake.name(),
            "Phone": fake.phone_number()[:20],
            "Email": fake.email(),
            "Address": fake.address().replace("\n", ",")[:200],
        })
    return pd.DataFrame(rows)


# 3. Products (cần supplier_ids)
CATEGORIES = ["Electronics", "Clothing", "Food", "Furniture", "Books", "Sports"]


def gen_products(n: int, supplier_ids: list) -> pd.DataFrame:
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "ProductID": i,
            "ProductName": fake.bs().title()[:100],
            "SupplierID": random.choice(supplier_ids),
            "Price": round(random.uniform(10000, 5000000), 0),
            "Category": random.choice(CATEGORIES),
        })
    return pd.DataFrame(rows)


# 4. Inventory (cần product_ids)
def gen_inventory(n: int, product_ids: list) -> pd.DataFrame:
    rows = []
    for i, pid in enumerate(product_ids, start = 1):
        rows.append({
            "InventoryID": i,
            "ProductID": pid,
            "QuantityInStock": random.randint(0, 500),
            "LastUpdated": fake.date_time_between(
                                start_date="-1y", end_date="now"
                           ).strftime("%Y-%m-%d %H:%M:%S"),
        })
    return pd.DataFrame(rows)


# 5. Order (cần customer_ids)
def gen_order(n: int, customer_ids: list) -> pd.DataFrame:
    rows = []
    start = datetime(2023, 1, 1)
    for i in range(1, n + 1):
        order_date = start + timedelta(days=random.randint(0, 730))
        rows.append({
            "OrderID": i,
            "CustomerID": random.choice(customer_ids),
            "OrderDate": order_date.strftime("%Y-%m-%d"),
            "TotalAmount": 0  # cập nhật sau khi có OrderDetails
        })
    return pd.DataFrame(rows)


# 6. OrderDetails (cần product_ids, order_ids, products dataframe để lấy giá)
def gen_order_details(n: int, order_ids: list, products_df: pd.DataFrame):
    rows = []
    price_map = dict(zip(products_df['ProductID'], products_df["Price"]))

    for i in range(1, n + 1):
        pid = random.choice(products_df['ProductID'].tolist())
        qty = random.randint(1, 20)
        unit_price = price_map[pid]

        rows.append({
            "OrderDetailID": i,
            "OrderID": random.choice(order_ids),
            "ProductID": pid,
            "Quantity": qty,
            "Price": round(unit_price * qty, 0),
        })
    return pd.DataFrame(rows)

# UPDATING TOTALAMOUNTS IN ORDER

def update_total_amount(conn, order_details_df: pd.DataFrame):
    # Tính lại TotalAmount cho mỗi Order từ OrderDetails
    totals = order_details_df.groupby("OrderID")["Price"].sum().reset_index()
    cursor = conn.cursor()
    for _, row in totals.iterrows():
        cursor.execute(
            "UPDATE Orders SET TotalAmount = ? WHERE OrderID = ?",
            float(row["Price"]), int(row["OrderID"])
        )
    conn.commit()
    print(f"Updated TotalAmount for {len(totals)} orders")


# RUNNING PIPELINE
def main():
    print(f"\n[{datetime.now():%H:%M:%S}] Connecting SSMS...")
    conn = get_connection()
    print(f"Connect Successfully!\n")

    truncate_all(conn)

    print(f"Generating and inserting data...")

    suppliers = gen_suppliers(N_SUPPLIERS)
    customers = gen_customers(N_CUSTOMERS)
    
    # Insert Suppilers
    insert_df(conn, "Suppliers", suppliers[["SupplierName", "Phone", "Address"]])

    # Đọc lại SupplierID thực từ Database
    cursor = conn.cursor()
    cursor.execute("SELECT SupplierID from Suppliers")
    real_supplier_ids = [row[0] for row in cursor.fetchall()]

    # Tạo Products với SupplierID thực
    products = gen_products(N_PRODUCTS, real_supplier_ids)
    inventory = gen_inventory(N_PRODUCTS, products["ProductID"].tolist())

    # Insert Customers
    insert_df(conn, "Customers", customers[["CustomerName", "Phone", "Email", "Address"]])

    # Đọc lại CustomerID thực từ database
    cursor.execute("SELECT CustomerID from Customers")
    real_customer_ids = [row[0] for row in cursor.fetchall()]

    # Tạo Orders với CustomerID thực
    order = gen_order(N_ORDERS, real_customer_ids)

    # Insert Products
    insert_df(conn, "Products", products[["ProductName", "SupplierID", "Price", "Category"]])

    # Đọc lại ProductID thực từ Database trước khi tạo Inventory
    cursor.execute("SELECT ProductID from Products")
    real_product_ids = [row[0] for row in cursor.fetchall()]

    # Tạo Inventory với ProductID thục từ database
    inventory = gen_inventory(len(real_product_ids), real_product_ids)
    insert_df(conn, "Inventory", inventory[["ProductID", "QuantityInStock", "LastUpdated"]])
    insert_df(conn, "Orders", order[["CustomerID", "OrderDate", "TotalAmount"]])

    
    # Đọc lại OrderID và ProductID thực từ Database
    cursor.execute("SELECT OrderID FROM Orders")
    real_order_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT ProductID, Price FROM Products")
    rows = cursor.fetchall()
    real_products_df = pd.DataFrame(list(map(tuple, rows)), columns=["ProductID", "Price"])

    # Tạo OrderDetails dùng ID thực từ database
    order_details = gen_order_details(N_ORDER_DETAILS, real_order_ids, real_products_df)
    insert_df(conn, "OrderDetails", order_details[["OrderID", "ProductID", "Quantity", "Price"]])

    update_total_amount(conn, order_details)

    conn.close()
    print(f"\n[{datetime.now():%H:%M:%S}] Done.")

if __name__ == "__main__":
    main()

