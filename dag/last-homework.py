from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow import DAG
import pandas as pd
import json
from pandas import json_normalize


def _process_data(ti):
    data = ti.xcom_pull(task_ids="extract_data")
    for index in range(0, len(data)):    
        processed_data = json_normalize({
            "show_name": data[index]["_embedded"]["show"]["name"],
            "type": data[index]["_embedded"]["show"]["type"],
            "series_name": data[index]["name"],
            "season_number": data[index]["season"],
            "series_number": data[index]["number"],
            "duration": data[index]["runtime"]
        })

        processed_data.to_csv("/tmp/processed_data.csv", index=None, header=False, mode="a")

def _store_show():
    hook = PostgresHook(postgres_conn_id="homework-postgres")
    hook.copy_expert(sql="COPY show_info FROM stdin WITH DELIMITER ',' ",
                     filename="/tmp/processed_data.csv")
    
def _group_data():
    hook = PostgresHook(postgres_conn_id="homework-postgres")
    conn = hook.get_conn()

    df = pd.read_sql("select * from show_info", conn)
    df = df.groupby(['show_name']).count()
    df = df['type']

    df.to_csv("/tmp/processed_show_group.csv", index_label=False, header=False)

def _store_show_group():
    hook = PostgresHook(postgres_conn_id="homework-postgres")
    hook.copy_expert(sql="COPY show_info_group FROM stdin WITH DELIMITER ',' ",
                     filename="/tmp/processed_show_group.csv")


with DAG(
    "postges-homework",
    start_date=datetime(2023, 5, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:
    create_table = PostgresOperator(
        postgres_conn_id="homework-postgres",
        task_id="create_table",
        sql="""
            CREATE TABLE IF NOT EXISTS show_info(
                show_name TEXT NOT NULL, 
                type TEXT NOT NULL, 
                series_name TEXT NOT NULL,  
                season_number INT NOT NULL, 
                series_number INT NOT NULL, 
                duration INT NOT NULL    
                );
        """
    )

    create_table_group= PostgresOperator(
        postgres_conn_id="homework-postgres",
        task_id="create_table_group",
        sql="""
            CREATE TABLE IF NOT EXISTS show_info_group(
                show_name TEXT NOT NULL, 
                count_serials INT NOT NULL    
                );
        """
    )

    extract_data = SimpleHttpOperator(
        task_id="extract_data",
        http_conn_id="homework-TVmazeAPI",
        endpoint='schedule/web?date={{ execution_date.strftime("%Y-%m-%d") }}&country=RU',
        method="GET",
        response_filter=(lambda response: json.loads(response.text)),
        log_response=True
    )

    process_data = PythonOperator(
        task_id="process_data",
        python_callable=_process_data
    )

    store_show = PythonOperator(
        task_id="store_show",
        python_callable=_store_show
    )

    group_data = PythonOperator(
        task_id="group_data",
        python_callable=_group_data   
    )

    store_show_group = PythonOperator(
        task_id="store_show_group",
        python_callable=_store_show_group
    )


    extract_data >> process_data >> store_show >> group_data >> store_show_group
    create_table_group
    create_table 