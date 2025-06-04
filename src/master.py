# -*- coding: utf-8 -*-
# +
import os
import cx_Oracle

# Load project properties
from config import get_properties

# Connection establishment
from connection import connect_to_oracle, connect_to_livy

# Fetch data from db
from data_extractor import fetch_db_data, fetch_smb_data, fetch_livy_data

# Process fetched data
from data_processor import process_data

# Save date in database
from data_loader import save_to_table, send_query
# -

# Load project properties
config = get_properties()

# Oracle Connection establishment
user = config['CIS.username']
password = config['CIS.password']
host = config['CIS.host']
port = config['CIS.port']
service_name = config['CIS.service_name']
livy_url = config['Livy.url']

# +
# Fetch data from db
with connect_to_oracle(username=user, password=password, host=host,
                               port=port, service_name=service_name) as oracle_conn:
    sql_query_1 = "SELECT * FROM AATTESTUSER.bind"
    data_1 = fetch_db_data(oracle_conn, sql_query_1)

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

