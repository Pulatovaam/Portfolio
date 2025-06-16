import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

K8S_SPARK_NAMESPACE = "de-project"
K8S_CONNECTION_ID = "kubernetes_karpov"
GREENPLUM_ID = "greenplume_karpov"

items_datamart_sql = """DROP EXTERNAL TABLE IF EXISTS "anastasija-pulatova-fqe3933".seller_items CASCADE;
CREATE EXTERNAL TABLE "anastasija-pulatova-fqe3933".seller_items(
    sku_id BIGINT,
    title TEXT,
    category TEXT,
    brand TEXT,
    seller TEXT,
    group_type TEXT,
    country TEXT,
    availability_items_count BIGINT,
    ordered_items_count BIGINT,
    warehouses_count BIGINT,
    item_price BIGINT,
    goods_sold_count BIGINT,
    item_rate FLOAT8,
    days_on_sell BIGINT,
    avg_percent_to_sold BIGINT,
    returned_items_count INTEGER,
    potential_revenue BIGINT,
    total_revenue BIGINT,
    avg_daily_sales FLOAT8,
    days_to_sold FLOAT8,
    item_rate_percent FLOAT8
) LOCATION ('pxf://startde-project/anastasija-pulatova-fqe3933/seller_items?PROFILE=s3:parquet&SERVER=default')
ON ALL FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';"""

unreliable_sellers_view_sql = """CREATE OR REPLACE VIEW "anastasija-pulatova-fqe3933".unreliable_sellers_view AS 
    SELECT seller, 
           SUM(availability_items_count) as total_overload_items_count,  
           BOOL_OR(
                availability_items_count > ordered_items_count 
                AND days_on_sell > 100
            ) AS is_unreliable
    FROM "anastasija-pulatova-fqe3933".seller_items
    GROUP BY seller;"""

brands_report_view_sql = """CREATE OR REPLACE VIEW "anastasija-pulatova-fqe3933".item_brands_view AS 
    SELECT brand,
           group_type,
           country, 
           SUM(potential_revenue)::FLOAT8 as potential_revenue,  
           SUM(total_revenue)::FLOAT8 as total_revenue,
           COUNT(sku_id)::BIGINT as items_count
    FROM "anastasija-pulatova-fqe3933".seller_items
    GROUP BY brand, group_type, country;"""



SUBMIT_NAME = "job_submit"


def _build_submit_operator(task_id: str, application_file: str, link_dag):
    return SparkKubernetesOperator(
        task_id=task_id,
        namespace=K8S_SPARK_NAMESPACE,
        application_file=application_file,
        kubernetes_conn_id=K8S_CONNECTION_ID,
        do_xcom_push=True,
        dag=link_dag
    )


def _build_sensor(task_id: str, application_name: str, link_dag):
    return SparkKubernetesSensor(
        task_id=task_id,
        namespace=K8S_SPARK_NAMESPACE,
        application_name=application_name,
        kubernetes_conn_id=K8S_CONNECTION_ID,
        attach_log=True,
        dag=link_dag
    )

default_args = {"owner": "anastasija-pulatova-fqe3933",}

with DAG(
    dag_id="startde-project-anastasija-pulatova-fqe3933-dag",
    default_args = default_args,
    schedule_interval=None,
    start_date=pendulum.datetime(2025, 5, 29, tz="UTC"),
    tags=["final_project"],
    catchup=False
) as dag:

    start = EmptyOperator(task_id="start", dag=dag)
    end = EmptyOperator(task_id="end", dag=dag)

    submit_task = _build_submit_operator(
        task_id=SUBMIT_NAME,
        application_file='spark_submit.yaml',
        link_dag=dag
    )

    sensor_task = _build_sensor(
        task_id='job_sensor',
        application_name=f"{{{{task_instance.xcom_pull(task_ids='{SUBMIT_NAME}')['metadata']['name']}}}}",
        link_dag=dag
    )

    create_table_items_datamart_task = SQLExecuteQueryOperator(
    task_id="items_datamart",
    conn_id = "greenplume_karpov",
    sql=items_datamart_sql,
    split_statements=True,
    return_last=False,
)
    
    create_unreliable_sellers_report_view = SQLExecuteQueryOperator(
    task_id="create_unreliable_sellers_report_view",
    conn_id = "greenplume_karpov",
    sql=unreliable_sellers_view_sql,
    split_statements=True,
    return_last=False,
)
    
    create_brands_report_view = SQLExecuteQueryOperator(
    task_id="create_brands_report_view",
    conn_id = "greenplume_karpov",
    sql=brands_report_view_sql,
    split_statements=True,
    return_last=False,
)

    start >> submit_task >> sensor_task >> create_table_items_datamart_task >> [create_unreliable_sellers_report_view, create_brands_report_view] >> end
