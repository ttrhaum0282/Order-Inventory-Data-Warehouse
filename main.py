import pyodbc
import random
from datetime import datetime, timedelta

def insert_suppliers(cursor):
    pass

def insert_customers(cursor):
    pass

def insert_products(cursor):
    pass

def insert_orders(cursor):
    pass

def insert_orderdetails(cursor):
    pass

def insert_inventory(cursor):
    pass

def main():
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost;"
        "DATABASE=Order_Inventory;"
        "Trusted_Connection=yes;"
    )

    cursor = conn.cursor()

    insert_suppliers(cursor)
    insert_customers(cursor)
    insert_products(cursor)
    insert_orders(cursor)
    insert_orderdetails(cursor)
    insert_inventory(cursor)

    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()