
#!/bin/bash

rsync -avz --include='**.gitignore' --exclude='/.git' --filter=':- .gitignore' --delete "/home/jovyan/workspace/ml_project//" "/home/jovyan/airflow/ml_project//" 


