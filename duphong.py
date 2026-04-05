import pyodbc
import random
from datetime import datetime, timedelta

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=Order_Inventory;"
    "Trusted_Connection=yes;"
)

cursor = conn.cursor()

supplier_names = ["TechSupply", "GlobalParts", "FastElectro", "MegaWholesale", "SmartSource"]
customer_names = ["An", "Binh", "Chi", "Dung", "Huy", "Lan", "Minh", "Nam", "Trang", "Vy"]
products = ["Laptop", "Keyboard", "Mouse", "Monitor", "Headphones", "Webcam"]

for name in supplier_names:
    cursor.execute("""
    INSERT INTO Suppliers (SupplierName, Phone, Address)
    VALUES (?, ?, ?)
    """, name, "090" + str(random.randint(1000000,9999999)), "Hanoi")

for name in customer_names:
    cursor.execute("""
    INSERT INTO Customers (CustomerName, Phone, Email, Address)
    VALUES (?, ?, ?, ?)
    """, name,
       "091" + str(random.randint(1000000,9999999)),
       name.lower()+"@gmail.com",
       "Vietnam")

for p in products:
    supplier_id = random.randint(1, len(supplier_names))
    price = random.randint(100, 2000)

    cursor.execute("""
    INSERT INTO Products (ProductName, SupplierID, Price, Category)
    VALUES (?, ?, ?, ?)
    """, p, supplier_id, price, "Electronics")

for product_id in range(1, len(products)+1):
    quantity = random.randint(10,100)

    cursor.execute("""
    INSERT INTO Inventory (ProductID, QuantityInSock, LastUpdated)
    VALUES (?, ?, GETDATE())
    """, product_id, quantity)

order_ids = []
for i in range(10):
    customer_id = random.randint(1, len(customer_names))
    total = random.randint(200,5000)

    cursor.execute("""
    INSERT INTO Orders (CustomerID, OrderDate, TotalAmount)
    OUTPUT INSERTED.OrderID
    VALUES (?, ?, ?)
    """, customer_id,
       datetime.now() - timedelta(days=random.randint(0,30)),
       total)

    order_id = cursor.fetchone()[0]
    order_ids.append(order_id)

for order_id in order_ids:
    product_id = random.randint(1, len(products))
    quantity = random.randint(1,5)
    price = random.randint(100,2000)

    cursor.execute("""
    INSERT INTO OrderDetails (OrderID, ProductID, Quantity, Price)
    VALUES (?, ?, ?, ?)
    """, order_id, product_id, quantity, price)

conn.commit()

print("Data generated successfully!")