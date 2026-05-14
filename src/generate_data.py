import pyodbc
import random
import os
import re
import unicodedata
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from unidecode import unidecode

fake = Faker('vi_VN')

# Tự sinh tên Việt đúng format "Họ Tên Đệm Tên", tách tên nam,nữ 
_HO = [
    'Nguyễn', 'Trần', 'Lê', 'Phạm', 'Hoàng', 'Huỳnh', 'Phan', 'Vũ', 'Võ', 'Đặng',
    'Bùi', 'Đỗ', 'Hồ', 'Ngô', 'Dương', 'Lý', 'Mai', 'Trịnh', 'Đinh', 'Tô',
]
_NAM = {
    'dem': ['Văn', 'Hữu', 'Đức', 'Minh', 'Quốc', 'Công', 'Bảo', 'Gia', 'Trung',
            'Tấn', 'Quang', 'Phú', 'Tiến', 'Thành', 'Phước', 'Anh', 'Xuân', 'Hải'],
    'ten': ['An', 'Bình', 'Dũng', 'Đạt', 'Giang', 'Hùng', 'Khoa', 'Khôi', 'Long',
            'Nam', 'Phong', 'Phúc', 'Quân', 'Sang', 'Toàn', 'Tuấn', 'Tùng', 'Việt'],
}
_NU = {
    'dem': ['Thị', 'Ngọc', 'Thu', 'Thúy', 'Thanh', 'Kim', 'Mỹ', 'Thùy', 'Bích',
            'Thảo', 'Diễm', 'Phương', 'Ánh', 'Khánh', 'Linh', 'Tú', 'Vân', 'Hà'],
    'ten': ['An', 'Chi', 'Dung', 'Giang', 'Hà', 'Hương', 'Lan', 'Linh', 'Mai',
            'Nga', 'Ngọc', 'Nhung', 'Thảo', 'Thư', 'Trâm', 'Trinh', 'Uyên', 'Yến',
            'Quỳnh', 'Trang', 'Phương', 'Tú', 'Vân'],
}

def vn_name(gender: str = None) -> tuple:
    """Sinh tên Việt đúng chuẩn. Trả về (full_name, gender)"""
    if gender is None:
        gender = random.choice(['M', 'F'])
    ho   = random.choice(_HO)
    pool = _NAM if gender == 'M' else _NU
    ten  = f"{random.choice(pool['dem'])} {random.choice(pool['ten'])}"
    return f"{ho} {ten}", gender


# Sinh email thực tế từ tên 
def name_to_email(full_name: str, uid: int, domain: str = "example.com") -> str:
    full_name = full_name.replace('Đ', 'D').replace('đ', 'd')
    
    nfkd = unicodedata.normalize("NFKD", full_name)
    ascii_name = nfkd.encode("ascii", "ignore").decode("ascii").lower()
    parts = ascii_name.split()          # ['nguyen', 'van', 'toan']
    ho    = parts[0]                    # nguyen
    dem   = parts[1] if len(parts) > 2 else ""   # van
    ten   = parts[-1]                   # toan
    birth = random.randint(1990, 2005)

    style = random.randint(1, 4) # ở đây chỉ tạo cấu trúc mail phổ biến
    if style == 1:
        # trinhtram2005@example.com  →  họ + tên + năm sinh
        local = f"{ho}{ten}{birth}"
    elif style == 2:
        # ttram2005@example.com  →  chữ đầu họ + tên + năm sinh
        local = f"{ho[0]}{ten}{birth}"
    elif style == 3:
        # nguyenvantoan99@example.com  →  họ + tên đệm + tên + 2 số cuối năm
        local = f"{ho}{dem}{ten}{str(birth)[-2:]}"
    else:
        # nvt2005@example.com  →  viết tắt chữ đầu + năm sinh
        initials = ho[0] + (dem[0] if dem else "") + ten[0]
        local = f"{initials}{birth}"

    local = re.sub(r"[^a-z0-9]", "", local)   # chỉ giữ chữ và số, không dấu chấm
    return f"{local}@{domain}"


# Sinh địa chỉ Việt Nam chuẩn 
_DUONG_PREFIX = ["Đường", "Phố", "Hẻm", "Ngõ", "Ngách"]
_TEN_DUONG = [
    "Lê Lợi", "Nguyễn Huệ", "Trần Phú", "Lý Thường Kiệt", "Đinh Tiên Hoàng",
    "Nguyễn Trãi", "Hoàng Diệu", "Phan Chu Trinh", "Bà Triệu", "Hai Bà Trưng",
    "Ngô Quyền", "Trần Hưng Đạo", "Võ Thị Sáu", "Cách Mạng Tháng 8", "Pasteur",
    "Nam Kỳ Khởi Nghĩa", "Điện Biên Phủ", "Lê Duẩn", "Hùng Vương", "Lê Hồng Phong",
    "Trường Chinh", "Giải Phóng", "Kim Mã", "Láng Hạ", "Nguyễn Chí Thanh",
]
_DON_VI = [
    ("Phường", "Quận 1",       "TP. Hồ Chí Minh"),
    ("Phường", "Quận 3",       "TP. Hồ Chí Minh"),
    ("Phường", "Quận 7",       "TP. Hồ Chí Minh"),
    ("Phường", "Bình Thạnh",   "TP. Hồ Chí Minh"),
    ("Phường", "Tân Bình",     "TP. Hồ Chí Minh"),
    ("Phường", "Hoàn Kiếm",    "Hà Nội"),
    ("Phường", "Đống Đa",      "Hà Nội"),
    ("Phường", "Cầu Giấy",     "Hà Nội"),
    ("Phường", "Hai Bà Trưng", "Hà Nội"),
    ("Phường", "Thanh Xuân",   "Hà Nội"),
    ("Phường", "Hải Châu",     "Đà Nẵng"),
    ("Phường", "Thanh Khê",    "Đà Nẵng"),
    ("Phường", "Ninh Kiều",    "Cần Thơ"),
    ("Xã",     "Hóc Môn",      "TP. Hồ Chí Minh"),
    ("Xã",     "Củ Chi",       "TP. Hồ Chí Minh"),
    ("Phường", "Lê Chân",      "Hải Phòng"),
    ("Phường", "Ngô Quyền",    "Hải Phòng"),
]

def vn_address(max_len: int = 150) -> str:
    so_nha = random.randint(1, 999)
    duong  = f"{random.choice(_DUONG_PREFIX)} {random.choice(_TEN_DUONG)}"
    don_vi, quan_huyen, tinh = random.choice(_DON_VI)
    phuong_so = random.randint(1, 20)
    addr = f"{so_nha} {duong}, {don_vi} {phuong_so}, {quan_huyen}, {tinh}"
    return addr[:max_len]


# Kết nối với SQL Server Management System
SERVER = 'localhost'
DATABASE = 'Order_Inventory'

CONN_STR = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"Trusted_Connection=yes;"
)

N_SUPPLIERS = 500
N_CUSTOMERS = 800
N_PRODUCTS = 500
N_ORDERS = 3000
N_ORDER_DETAILS = 4800


def get_connection():
    return pyodbc.connect(CONN_STR)

def get_engine():
    conn_url = (
        "mssql+pyodbc://localhost/Order_Inventory"
        "?driver=ODBC+Driver+17+for+SQL+Server"
        "&Trusted_Connection=yes"
    )
    return create_engine(conn_url)

def truncate_all(conn):
    cursor = conn.cursor()
    deletes = [
        "DELETE FROM OrderDetails",
        "DELETE FROM Orders",
        "DELETE FROM Inventory",
        "DELETE FROM Products",
        "DELETE FROM Customers",
        "DELETE FROM Suppliers",
    ]
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
# 1. Suppliers
def gen_suppliers(n: int) -> pd.DataFrame:
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "SupplierID": i,
            "SupplierName": fake.company(),
            "Phone": fake.phone_number()[:20],
            "Address": vn_address()[:200],
        })
    return pd.DataFrame(rows)


# 2. Customers  ── ÁP DỤNG 2 FIX Ở ĐÂY ──
def gen_customers(n: int) -> pd.DataFrame:
    rows = []
    for i in range(1, n + 1):
        name, _gender = vn_name()                       # FIX 1: tên đúng chuẩn, tách nam/nữ
        rows.append({
            "CustomerID":   i,
            "CustomerName": name,
            "Phone":        fake.phone_number()[:20],
            "Email":        name_to_email(name, i),        # FIX 2: email thực tế từ tên
            "Address":      vn_address()[:200],
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
    for i, pid in enumerate(product_ids, start=1):
        rows.append({
            "InventoryID": i,
            "ProductID": pid,
            "QuantityInStock": random.randint(0, 500),
            "LastUpdated": fake.date_time_between(
                                start_date="-1y", end_date="now"
                           ).strftime("%Y-%m-%d %H:%M:%S"),
        })
    return pd.DataFrame(rows)


# 5. Orders (cần customer_ids)
def gen_order(n: int, customer_ids: list) -> pd.DataFrame:
    rows = []
    start = datetime(2023, 1, 1)
    for i in range(1, n + 1):
        order_date = start + timedelta(days=random.randint(0, 730))
        rows.append({
            "OrderID": i,
            "CustomerID": random.choice(customer_ids),
            "OrderDate": order_date.strftime("%Y-%m-%d"),
            "TotalAmount": 0
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
    totals = order_details_df.groupby("OrderID")["Price"].sum().reset_index()
    cursor = conn.cursor()
    for _, row in totals.iterrows():
        cursor.execute(
            "UPDATE Orders SET TotalAmount = ? WHERE OrderID = ?",
            float(row["Price"]), int(row["OrderID"])
        )
    conn.commit()
    print(f"Updated TotalAmount for {len(totals)} orders")


# Tạo file csv
def export_all(fmt: str = "csv"):
    engine = get_engine()
    tables = ["Suppliers", "Customers", "Products", "Inventory", "Orders", "OrderDetails"]
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(base_dir, "output")
    os.makedirs(output_dir, exist_ok=True)
    
    for table in tables:
        df = pd.read_sql(f"SELECT * FROM {table}", engine)
        if fmt == "csv":
            df.to_csv(os.path.join(output_dir, f"{table}.csv"), index=False, encoding="utf-8-sig")
        else:
            df.to_json(os.path.join(output_dir, f"{table}.json"), orient="records", force_ascii=False, indent=2)
        print(f"Exported {table}.{fmt} ({len(df)} rows)")


# RUNNING PIPELINE
def main():
    print(f"\n[{datetime.now():%H:%M:%S}] Connecting SSMS...")
    conn = get_connection()
    print(f"Connect Successfully!\n")

    truncate_all(conn)

    print(f"Generating and inserting data...")

    suppliers = gen_suppliers(N_SUPPLIERS)
    customers = gen_customers(N_CUSTOMERS)
    
    insert_df(conn, "Suppliers", suppliers[["SupplierName", "Phone", "Address"]])

    cursor = conn.cursor()
    cursor.execute("SELECT SupplierID from Suppliers")
    real_supplier_ids = [row[0] for row in cursor.fetchall()]

    products  = gen_products(N_PRODUCTS, real_supplier_ids)
    inventory = gen_inventory(N_PRODUCTS, products["ProductID"].tolist())

    insert_df(conn, "Customers", customers[["CustomerName", "Phone", "Email", "Address"]])

    cursor.execute("SELECT CustomerID from Customers")
    real_customer_ids = [row[0] for row in cursor.fetchall()]

    order = gen_order(N_ORDERS, real_customer_ids)

    insert_df(conn, "Products", products[["ProductName", "SupplierID", "Price", "Category"]])

    cursor.execute("SELECT ProductID from Products")
    real_product_ids = [row[0] for row in cursor.fetchall()]

    inventory = gen_inventory(len(real_product_ids), real_product_ids)
    insert_df(conn, "Inventory", inventory[["ProductID", "QuantityInStock", "LastUpdated"]])
    insert_df(conn, "Orders", order[["CustomerID", "OrderDate", "TotalAmount"]])

    cursor.execute("SELECT OrderID FROM Orders")
    real_order_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT ProductID, Price FROM Products")
    rows = cursor.fetchall()
    real_products_df = pd.DataFrame(list(map(tuple, rows)), columns=["ProductID", "Price"])

    order_details = gen_order_details(N_ORDER_DETAILS, real_order_ids, real_products_df)
    insert_df(conn, "OrderDetails", order_details[["OrderID", "ProductID", "Quantity", "Price"]])

    update_total_amount(conn, order_details)

    export_all(fmt="csv")

    conn.close()
    print(f"\n[{datetime.now():%H:%M:%S}] Done.")


if __name__ == "__main__":
    main()