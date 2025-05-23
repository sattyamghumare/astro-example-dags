from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.utils.dates import days_ago
 
with DAG(
    dag_id="aws_services_etl_dags",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["aws", "lambda", "glue"]
) as dag:
 
    invoke_lambda = LambdaInvokeFunctionOperator(
        task_id="invoke_lambda_function",
        function_name="lambda_bank_etl",
        aws_conn_id="aws_conn_sattyamg"
    )
 
    run_glue = GlueJobOperator(
        task_id="run_glue_job",
        job_name="glue_bank_transformation",
        aws_conn_id="aws_default",
        region_name="us-east-1"
    )
 
    invoke_lambda >> run_glue
 
