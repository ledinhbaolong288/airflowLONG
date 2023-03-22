from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from dotenv import load_dotenv
from plugins.Athena import processAthena
from plugins.QueryFromSQLServer import GetSQL, LoadDataSQL
from plugins.Log import Log_process
load_dotenv()


dag_store = DAG(
    dag_id="STORE_TABLE",
    default_args={
        "owner": "LONGLDB",
        "depends_on_past": False,
        "start_date": datetime(2023, 3, 6),
        "retries": 1,
        # "retry_delay": timedelta(minutes=2),
    },
    schedule_interval=None
)
dag_class = DAG(
    dag_id="CLASS_TABLE",
    default_args={
        "owner": "LONGLDB",
        "depends_on_past": False,
        "start_date": datetime(2023, 3, 6),
        "retries": 1,
        # "retry_delay": timedelta(minutes=2),
    },
    schedule_interval=None
)
dag_group = DAG(
    dag_id="GROUP_TABLE",
    default_args={
        "owner": "LONGLDB",
        "depends_on_past": False,
        "start_date": datetime(2023, 3, 6),
        "retries": 1,
        # "retry_delay": timedelta(minutes=2),
    },
    schedule_interval=None
)


def ETL_CLASS():
    try:
        table = 'CLASS'
        infor, conn = GetSQL(table)
        data = processAthena(
            infor['QUERY'], infor['AWS_ACCESS_KEY_ID'], infor['AWS_SECRET_ACCESS_KEY'])
        LoadDataSQL(data, table)
        Log_process(conn, "0")
    except ZeroDivisionError:
        Log_process(conn, "1", ZeroDivisionError)
    except:
        Log_process(conn, "1")


def ETL_STORE():
    # try:
    table = 'STORE'
    infor, conn = GetSQL(table)
    print("data=", infor)
    print("table_name", infor['TABLE_NAME'])

    # data = processAthena(
    #     infor['QUERY'], infor['AWS_ACCESS_KEY_ID'], infor['AWS_SECRET_ACCESS_KEY'])
    # LoadDataSQL(data, table)
    # Log_process(conn, "0")
    # except ZeroDivisionError:
    #     Log_process(conn, "1", ZeroDivisionError)
    # except:
    #     Log_process(conn, "1")


def ETL_GROUP():
    try:
        table = 'GROUP'
        infor, conn = GetSQL(table)
        data = processAthena(
            infor['QUERY'], infor['AWS_ACCESS_KEY_ID'], infor['AWS_SECRET_ACCESS_KEY'])
        LoadDataSQL(data, table)
        Log_process(conn, "0")
    except ZeroDivisionError:
        Log_process(conn, "1", ZeroDivisionError)
    except:
        Log_process(conn, "1")


run_ETL_STORE = PythonOperator(
    task_id="run_etl_store",
    python_callable=ETL_STORE,
    dag=dag_store
)

run_ETL_CLASS = PythonOperator(
    task_id="run_etl_class",
    python_callable=ETL_CLASS,
    dag=dag_class
)
run_ETL_GROUP = PythonOperator(
    task_id="run_etl_group",
    python_callable=ETL_GROUP,
    dag=dag_group
)

start_operator_store = DummyOperator(task_id='Begin_execution', dag=dag_store)
end_operator_store = DummyOperator(task_id='Stop_execution',  dag=dag_store)
start_operator_class = DummyOperator(task_id='Begin_execution', dag=dag_class)
end_operator_class = DummyOperator(task_id='Stop_execution',  dag=dag_class)
start_operator_group = DummyOperator(task_id='Begin_execution', dag=dag_group)
end_operator_group = DummyOperator(task_id='Stop_execution',  dag=dag_group)
start_operator_store >> run_ETL_STORE >> end_operator_store
start_operator_class >> run_ETL_CLASS >> end_operator_class
start_operator_group >> run_ETL_GROUP >> end_operator_group
