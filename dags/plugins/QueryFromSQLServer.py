import pyodbc
import os
import pandas as pd
from sqlalchemy import create_engine


def GetSQL(table):  # S3-HIS 1
    conn = pyodbc.connect('Driver={ODBC Driver 18 for SQL Server};'
                          'Server=172.27.32.1;'  # tên máy chủ SQL Server
                          'Database=STAGING;'  # tên cơ sở dữ liệu
                          'UID=airflow;'
                          'PWD=airflow;'
                          'TrustServerCertificate=yes;',
                          )
    # query = pd.read_sql(f"select QUERY from {table}")
    # conn.commit()

    # engine = create_engine(
    #     f"mssql+pyodbc://airflow:airflow@172.27.32.1/STAGING?driver=ODBC Driver 18 for SQL Server&TrustServerCertificate=yes")
    # query = f"""
    #   select * from S3_HIS where TABLE_NAME='{table}' and SERVER = 'A' and STATUS = 'ACTIVATE';
    # """
    query = f"""
      select * from S3_HIS where table_name = '{table}' and server='A'
    """
    data = pd.read_sql(query, conn)
    return data, conn


def LoadDataSQL(data, table):
    # conn = create_engine(
    #     f'mssql+pyodbc://172.20.112.1//STAGING?')
    conn = pyodbc.connect('Driver={ODBC Driver 18 for SQL Server};'
                          'Server=172.27.32.1;'  # tên máy chủ SQL Server
                          'Database=STAGING;'  # tên cơ sở dữ liệu
                          'UID=airflow;'
                          'PWD=airflow;'
                          'TrustServerCertificate=yes;'
                          )
    if table == "STORE":
        for row in data:
            conn.execute("""
              INSERT INTO Store(Store_ID,Store_Sales,Daily_Customer_Count,Items_Available,Store_Area)
              VALUES (?,?,?,?,?)
              """, row['store_id'], row['store_sales'], row['daily_customer_count'], row['items_available'], row['store_area'])
            conn.commit()

    elif table == "CLASS":
        for row in data:
            conn.execute("""
              INSERT INTO Store(Store_ID,Store_Sales,Daily_Customer_Count,Items_Available,Store_Area)
              VALUES (?,?,?,?,?)
              """, row['store_id'], row['store_sales'], row['daily_customer_count'], row['items_available'], row['store_area'])
            conn.commit()

    elif table == "GROUP":
        for row in data:
            conn.execute("""
              INSERT INTO Store(Store_ID,Store_Sales,Daily_Customer_Count,Items_Available,Store_Area)
              VALUES (?,?,?,?,?)
              """, row['store_id'], row['store_sales'], row['daily_customer_count'], row['items_available'], row['store_area'])
            conn.commit()
