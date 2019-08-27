from airflow.models import DAG, Variable
from datetime import datetime, timedelta
from airflow.operators.postgres_operator import PostgresOperator
 
ENV = Variable.get("ENV")
NOTIFY_EMAIL = ['karthikeya.chennuru@outlook.com'] if ENV == 'PROD' else ['karthikeya.chennuru@outlook.com']
 
POSTGRES_CONN_ID = 'redshiftdb'
start_date = datetime(2018, 8, 7)
 
args = {
    'owner': 'kchennuru',
    'start_date': start_date,
    'depends_on_past': False,
    'email': NOTIFY_EMAIL,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'domain': 'digital marketing',
    'retry_delay': timedelta(minutes=3),
}
 
dag = DAG(
    dag_id='example_redshift_daily_process',
    default_args=args,
    schedule_interval="@daily",
    catchup=False
)
dag.doc_md = """
load prod redshift table from prod athena daily
"""

sql_str=""" copy intake.employee
from 's3://xyz-ingest-employee/yr=2019/mo=08/dt=2019-08-26/employee.csv'
iam_role 'arn:aws:iam::591261435815:role/RedshiftSpectrumRole'
FORMAT AS CSV 
DELIMITER AS '|'
region 'us-east-1';"""


# load to redshift from athena table
load_into_redshift = PostgresOperator(
    postgres_conn_id=POSTGRES_CONN_ID,
    pool='redshift',
    task_id="load_employee",
    sql=sql_str,
    priority_weight=20000,
    dag=dag)
 
 
 
