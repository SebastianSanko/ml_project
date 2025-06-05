
#Load libraries
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import os


#### REQUIRED ! DO NOT CHANGE #################################
# Define path with Py scripts
work_env = os.getenv("WORK_ENV", "SANDBOX")
source_path = os.environ.get('PROJECTS_PATH', os.environ.get('AIRFLOW_DAGS_DIR'))
source_path = source_path if work_env == "SANDBOX" else source_path + '/${airflow_path}'


#######################################

# Define notebook to execute via papermill
input_notebook = f"{source_path}/ml_project/src/process.ipynb"

# Define notebook with results (created after execution input notebook)
output_notebook = f"{source_path}/ml_project/src/outputs/process_output_{datetime.now().strftime('%Y_%m_%d-%I_%M_%S_%p')}.ipynb"



default_args = {
    'owner': 'myadmin',
    'start_date': datetime(2024, 5, 1)
	# Add some args here ...
}

# Name of virtualenv - you can check by typing `conda env list` in console
env_name = "base"

# Kernel name
kernel_name = "conda-base-py"

# You should number your dags.
# Name of dag must be unique.
dag = DAG(
    'NOTEBOOK_DAG_ml_project_bDzEASaHmO_1',
    default_args=default_args,
    description='desc',
    schedule_interval='@daily',
    catchup=False,
)

# Tasks definition
run_notebook_script = BashOperator(
    task_id='run_notebook_script',
    bash_command=f'source activate base && conda activate {env_name} && cd {source_path}/ml_project/src && mkdir -p outputs && papermill {input_notebook} {output_notebook} -k {kernel_name}',
    dag=dag
)

# 
run_notebook_script
