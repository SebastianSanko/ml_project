from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
import os

# Ustawienia środowiska
work_env = os.getenv("WORK_ENV", "SANDBOX")
base_path = os.environ.get('PROJECTS_PATH', os.environ.get('AIRFLOW_DAGS_DIR', '/opt/airflow/dags'))
source_path = base_path if work_env == "SANDBOX" else os.path.join(base_path, os.getenv('airflow_path', ''))

default_args = {
    'owner': 'myadmin',
    'start_date': datetime(2024, 5, 1),
    'retries': 1
}

env_name = "base"            # Nazwa środowiska Conda
script_path = f"{source_path}/ml_project/src/master.py"

dag = DAG(
    'DAG_ml_project_bDzEASaHmO_1',
    default_args=default_args,
    description='Run master.py from src with Conda environment',
    schedule_interval='@daily',
    catchup=False,
)

run_master_script = BashOperator(
    task_id='run_master_script',
    bash_command=(
        f'source activate base && conda activate {env_name} && '
        f'python {script_path}'
    ),
    dag=dag
)

run_master_script
