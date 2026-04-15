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
    f"SERVER={localhost};"
    f"DATABASE={Order_Inventory};"
    f"Trusted_Connection=yes;"
)

N_SUPPLIERS = 20
N_CUSTOMERS = 50
N_PRODUCTS = 80
N_ORDERS = 200
N_ORDER_DETAILS = 400

def get_connection():
    return pyodbc.connect(CONN_STR)

def insert_df(conn, table: str, df: pd.DataFrame):
    cursor = conn.cursor()
    cols = ", ".join(df.columns)
    placeholders = ", ".join(["?"] * len(df.columns))
    sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
    cursor.executetemany(sql, df.values.tolist())
    conn.commit()
    print(f"Inserted {len(df)} rows -> {table}")

# GENERATING DATA

# 1.Suppliers
def gen_suppliers(n: int) -> pd.DataFrame:
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "SupplerID": i,
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
    for i in range(1, n + 1):
        rows.append({
            "InventoryID": i,
            "ProductID": random.choice(product_ids),
            "QuantityInStock": random.randint(0, 500),
            "LastUpdated": fake.date_time_between(
                                start_date = "-1y", end_date = "now"
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
            "TotalAmount": 0 #cập nhật sau khi có OrderDetails
        })
    return pd.DateFrame(rows)

# 6. OrderDetails (cần product_ids, order_ids, products dataframe để lấy giá)
def gen_order_details(n: int, product_ids: list, order_ids: list, products_df: pd.DataFrame):
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


#RUNNING PIPELINE
def main():
    print(f"\n[{datetime.now():%H:%M:%S}] Connecting SSMS...")
    conn = get_connection()
    print(f"Connect Successfully!")

    print(f"Generating and inserting data...")

    suppliers = gen_suppliers(N_SUPPLIERS)
    customers = gen_customers(N_CUSTOMERS)
    products = gen_products(N_PRODUCTS, suppliers["SupplierID"].tolist())
    inventory = gen_inventory(products["ProductID"].tolist())
    order = gen_order(N_ORDERS, customers["CustomerID"].tolist())
    order_details = gen_order_details(N_ORDER_DETAILS, 
                                      order["OrderID"].tolist(),
                                      products)
    
    # Insert theo thứ tự
    insert_df(conn, "Suppliers", suppliers[["SupplierID", "SupplierName", "Phone", "Address"]])

    insert_df(conn, "Customers", customers[["CustomerID", "CustomerName", "Phone", "Email", "Address"]])

    insert_df(conn, "Products", products[["ProductID", "ProductName", "SupplierID", "Price", "Category"]])

    insert_df(conn, "Inventory", inventory[["InventoryID", "ProductID", "QuantityInStock", "LastUpdated"]])

    insert_df(conn, "Order", order[["OrderID", "CustomerID", "OrderDate", "TotalAmount"]])

    insert_df(conn, "OrderDetails", order_details[["OrderDetailID", "OrderID", "ProductID", "Quantity", "Price"]])

    # Cập nhật TotalAmount
    update_total_amount(conn, order_details)

    conn.close()
    print(f"\n[{datetime.now():%H:%M:%S}] Done.")
    print("\nKiểm tra nhanh bằng SQL:")
    print("  SELECT COUNT(*) FROM Suppliers")
    print("  SELECT COUNT(*) FROM Orders")
    print("  SELECT TOP 5 * FROM OrderDetails")
 
 if __name__ == "__main__":
    main()

