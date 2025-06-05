# -*- coding: utf-8 -*-
# +
import os
from pathlib import Path
# Load project properties
from config import get_properties

# Connection establishment
from connection import connect_to_db

# Process fetched data
from data_processor import process_data

# Load data
from data_loader import generate_random_diabetes_data
# -

# Load project properties
config = get_properties()

# Oracle Connection establishment
user = config['DB.username']
password = config['DB.password']
host = config['DB.host']
port = config['DB.port']
service_name = config['DB.service_name']
livy_url = config['Livy.url']

# +
# Fetch data from db
conn = connect_to_db(username=user, password=password, host=host,
                               port=port, service_name=service_name)

data_1 = generate_random_diabetes_data(n_rows=5, seed=42)

# +
# Process fetched data
processed_data_1 = process_data(data_1)

#You can use it with another dataset
# ...
# processed_data_2 <- process_data(data_2)
# ...
# -

# You pick which function to run or add another one in write_to_oracle.py script
# send_query(oracle_conn, "drop table TEST")
# save_to_table(oracle_conn, data_3, "TEST", overwrite=True, create_if_not_exist=True)
# send_query(connection, "create table test as select 1 as col from dual")
# execute_stored_procedure(connection, "GRANT SELECT ON Table TO user")

# Ścieżka do folderu tego skryptu
BASE_DIR = Path(__file__).resolve().parent

# Pełna ścieżka do pliku
data_path = BASE_DIR / '../data/data.csv'
processed_data_1.to_csv(data_path)
