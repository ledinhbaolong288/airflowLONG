FROM apache/airflow:2.5.2
RUN pip install apache-airflow-providers-microsoft-mssql
RUN pip install pyathena
RUN pip install pyodbc pymssql
RUN pip install pyspark
RUN pip install sqlserverport
RUN pip install getenv