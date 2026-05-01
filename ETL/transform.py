import pandas as pd
import os
from datetime import datetime

INPUT_DIR = r"D:\Projects_TTCS\src\output"   # CSV từ generate_data.py
OUTPUT_DIR = "transformed"

os.makedirs(OUTPUT_DIR, exist_ok=True)


def load_csv(table: str) -> pd.DataFrame:
    path = os.path.join(INPUT_DIR, f"{table}.csv")
    return pd.read_csv(path, encoding="utf-8-sig")


def save_csv(df: pd.DataFrame, name: str):
    path = os.path.join(OUTPUT_DIR, f"{name}.csv")
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"Saved {name}.csv ({len(df)} rows)")


# 1. Suppliers
def transform_suppliers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["SupplierID"])
    df["SupplierName"] = df["SupplierName"].str.strip().str.title()
    
    # Chuẩn hóa phone về đầu 0
    df["Phone"] = df["Phone"].str.replace(r"[\s\-\(\)]", "", regex=True)
    df["Phone"] = df["Phone"].str.replace(r"^\+84", "0", regex=True)
    
    return df


# 2. Customers
def transform_customers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["CustomerID"])
    df["CustomerName"] = df["CustomerName"].str.strip()  # bỏ .str.title() với tiếng Việt
    df["Email"] = df["Email"].str.lower().str.strip()

    # Chuẩn hóa phone về dạng 0xxxxxxxxx 
    df["Phone"] = df["Phone"].str.replace(r"[\s\-\(\)]", "", regex=True)
    df["Phone"] = df["Phone"].str.replace(r"^\+84", "0", regex=True)

    # Validate email
    email_mask = df["Email"].str.contains(r"^[\w\.-]+@[\w\.-]+\.\w+$", regex=True)
    df = df[email_mask].copy()

    # Validate phone (chỉ giữ số hợp lệ 10 chữ số bắt đầu bằng 0)
    phone_mask = df["Phone"].str.match(r"^0\d{9}$")
    df = df[phone_mask].copy()

    return df


# 3. Products
def transform_products(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["ProductID"])

    # Loại sản phẩm giá âm hoặc null
    df = df[df["Price"] > 0].copy()

    # Chuẩn hóa Category
    valid_categories = ["Electronics", "Clothing", "Food", "Furniture", "Books", "Sports"]
    df["Category"] = df["Category"].str.strip().str.title()
    df = df[df["Category"].isin(valid_categories)].copy()

    # Thêm cột phân khúc giá
    df["PriceSegment"] = pd.cut(
        df["Price"],
        bins=[0, 100_000, 500_000, 2_000_000, float("inf")],
        labels=["Budget", "Mid", "Premium", "Luxury"]
    )
    return df


# 4. Inventory 
def transform_inventory(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["InventoryID"])
    df["QuantityInStock"] = df["QuantityInStock"].clip(lower=0)
    df["LastUpdated"] = pd.to_datetime(df["LastUpdated"], errors="coerce")
    df = df.dropna(subset=["LastUpdated"])

    # Thêm trạng thái tồn kho
    df["StockStatus"] = pd.cut(
        df["QuantityInStock"],
        bins=[-1, 0, 50, 200, float("inf")],
        labels=["Out of Stock", "Low", "Normal", "High"]
    )
    return df


# 5. Orders 
def transform_orders(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["OrderID"])
    df["OrderDate"] = pd.to_datetime(df["OrderDate"], errors="coerce")
    df = df.dropna(subset=["OrderDate"])
    df["TotalAmount"] = df["TotalAmount"].clip(lower=0)

    # Thêm chiều thời gian
    df["OrderYear"]    = df["OrderDate"].dt.year
    df["OrderMonth"]   = df["OrderDate"].dt.month
    df["OrderQuarter"] = df["OrderDate"].dt.quarter
    df["OrderWeekday"] = df["OrderDate"].dt.day_name()
    return df


# 6. OrderDetails
def transform_order_details(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["OrderDetailID"])
    df = df[(df["Quantity"] > 0) & (df["Price"] >= 0)].copy()

    # Tính unit price thực tế
    df["UnitPrice"] = (df["Price"] / df["Quantity"]).round(0)
    return df


# 7. Bảng tổng hợp (Fact/Summary) 
def build_sales_summary(
    orders: pd.DataFrame,
    order_details: pd.DataFrame,
    products: pd.DataFrame,
    customers: pd.DataFrame,
) -> pd.DataFrame:
    df = order_details.merge(orders[["OrderID", "CustomerID", "OrderDate",
                                     "OrderYear", "OrderMonth", "OrderQuarter"]],
                             on="OrderID", how="left")
    df = df.merge(products[["ProductID", "ProductName", "Category", "PriceSegment"]],
                  on="ProductID", how="left")
    df = df.merge(customers[["CustomerID", "CustomerName"]],
                  on="CustomerID", how="left")
    return df


# Main 
def main():
    print(f"\n[{datetime.now():%H:%M:%S}] Starting transform...\n")

    suppliers    = transform_suppliers(load_csv("Suppliers"))
    customers    = transform_customers(load_csv("Customers"))
    products     = transform_products(load_csv("Products"))
    inventory    = transform_inventory(load_csv("Inventory"))
    orders       = transform_orders(load_csv("Orders"))
    order_details= transform_order_details(load_csv("OrderDetails"))

    sales_summary = build_sales_summary(orders, order_details, products, customers)

    save_csv(suppliers,     "Suppliers")
    save_csv(customers,     "Customers")
    save_csv(products,      "Products")
    save_csv(inventory,     "Inventory")
    save_csv(orders,        "Orders")
    save_csv(order_details, "OrderDetails")
    save_csv(sales_summary, "SalesSummary")

    print(f"\n[{datetime.now():%H:%M:%S}] Transform done.")


if __name__ == "__main__":
    main()