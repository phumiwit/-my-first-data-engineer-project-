# 1.ไลบรารีที่จำเป็น
import glob
from datetime import timedelta, datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators import dataproc_operator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils import trigger_rule
#2. กำหนดวันเริ่มต้น
#ในกรณีนี้คือเมื่อวานนี้ 
yesterday = datetime(2023, 5, 12)


PROJECT_ID ='datapipeline-withdatapoc-v1'
REGION = 'asia-east2'
CLUSTERNAME ='dataproc-cluster-cleansing-data-{{ ds_nodash }}'
PYSPARK_URI ='gs://spark-job-cleansingdata/tranformation.py'

# CLUSTER_CONFIG
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
    },
}

# PYSPARK_JOB
PYSPARK_JOB = {
    "reference":{"project_id":PROJECT_ID},
    "placement":{"cluster_name":CLUSTERNAME},
    "pyspark_job":{"main_python_file_uri":PYSPARK_URI}
}

# dataproc job name
dataproc_job_name = 'spark_job_dataproc'
#3. Set default arguments for the DAG
default_dag_args = {
'start_date': yesterday,
'depends_on_past': False,
'email_on_failure': False,
'email_on_retry': False,
'retries': 1,
'retry_delay': timedelta(minutes=5),
'project_id': PROJECT_ID
}

#4. Define DAG
# set the DAG name, add a DAG description, define the schedule interval and pass the default arguments defined before
with models.DAG(
'spark_workflow',
description='DAG for deployment a Dataproc Cluster',
schedule_interval=timedelta(days=1),
default_args=default_dag_args) as dag:
# 5. Set Operators
# BashOperator
# แสดงวันที่
    print_date = BashOperator(
        task_id='print_date',
        bash_command='date'
        )
    # dataproc_operator
    # สร้าง dataproc cluster
    create_dataproc = dataproc_operator.DataprocCreateClusterOperator(
        task_id='create_dataproc',
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTERNAME
    )
    # Run PySpark job
    run_spark = DataprocSubmitJobOperator(
        task_id='run_spark',
        job=PYSPARK_JOB,
        region=REGION,
        project_id = PROJECT_ID
    )
    # dataproc_operator
    # ลบ dataproc cluster
    delete_dataproc = dataproc_operator.DataprocDeleteClusterOperator(
        task_id='delete_dataproc',
        region=REGION,
        cluster_name=CLUSTERNAME,
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE
    )
    # 6. สร้าง Bigquery และบันทึกข้อมูลที่ทำการ clean แล้ว
    load_to_bg = GCSToBigQueryOperator(
        task_id='gcs_to_bq',
        bucket='us-central1-data-pipeline-c-a54e9a00-bucket',
        source_objects=['data/retail/retail_updated.csv/*.csv'],
        destination_project_dataset_table='retail.retail_data',
        skip_leading_rows=1,
        
        schema_fields=[
            {
                "mode": "NULLABLE",
                "name": "description",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "customerid",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "country",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "timestamp",
                "type": "TIMESTAMP"
            },
            {
                "mode": "NULLABLE",
                "name": "invoiceno",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "quantity",
                "type": "INTEGER"
            },
            {
                "mode": "NULLABLE",
                "name": "unitprice",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "stockcode",
                "type": "STRING"
            }
        ],
        write_disposition='WRITE_TRUNCATE',
        
    )

    # 7. Set DAGs dependencies
    print_date >> create_dataproc >> run_spark >> delete_dataproc >> load_to_bg