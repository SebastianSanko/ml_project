
#!/bin/bash

rsync -avz --include='**.gitignore' --exclude='/.git' --filter=':- .gitignore' --delete "/home/jovyan/workspace/proj//" "/home/jovyan/airflow/proj//" 


