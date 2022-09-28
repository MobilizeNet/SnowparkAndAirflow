from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from random import randint
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


def _choosing_best_model(ti):
    accuracies = ti.xcom_pull(task_ids=[
        'training_model_A',
        'training_model_B',
        'training_model_C'
    ])
    if max(accuracies) > 8:
        return 'accurate'
    return 'inaccurate'
    
def _training_model(model):
    return randint(1, 10)

def submit(application,py_files,task_id):
    def execute():
        hook = SnowflakeHook('snowflake-demo')
        conn = hook.get_conn()
        from snowflake.snowpark import Session
        import sys
        sys.path.append(py_files)
        session = Session.builder.config("connection", conn).create() 
        exec(open(application).read())
    return PythonOperator(task_id=task_id, python_callable=execute)


with DAG("my_dag_snowflake", start_date=datetime(2021, 1 ,1), schedule_interval='@daily', catchup=False) as dag:

    snowpark_submit_task = submit(
		application ='/home/BlackDiamond/workspace/snowpark_sample1/driver.py' ,
        py_files='/home/BlackDiamond/workspace/snowpark_sample1/dist/snowpark_sample1-0.0.1-py3.8.egg',
		task_id='snowpark_submit_task'
	)


    training_model_tasks = [
        PythonOperator(
            task_id=f"training_model_{model_id}",
            python_callable=_training_model,
            op_kwargs={
            "model": model_id
            }
        ) for model_id in ['A', 'B', 'C']
    ]
    choosing_best_model = BranchPythonOperator(
        task_id="choosing_best_model",
        python_callable=_choosing_best_model)
    accurate = BashOperator(
        task_id="accurate",
        bash_command="echo 'accurate'")
    inaccurate = BashOperator(
        task_id="inaccurate",
        bash_command=" echo 'inaccurate'")
    snowpark_submit_task >> training_model_tasks >> choosing_best_model >> [accurate, inaccurate]
