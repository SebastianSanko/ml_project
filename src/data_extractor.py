# +
import pandas as pd
from smbclient import listdir, mkdir, register_session, rmdir, scandir, open_file
from livy import LivySession
from requests_kerberos import HTTPKerberosAuth
import requests
import os

def fetch_db_data(connection, query):
    """
    Function to fetch data from the Oracle database.

    Args:
        connection (Connection): Connection to the Oracle database.
        query (str): SQL query to fetch data.

    Returns:
        DataFrame: Data fetched from the database.
    """
    # Execute sql query and get result
    df = pd.read_sql(query, con=connection)

    # Other operations related to the fetch data
    # ...
    # ...
    # ...

    # Result return
    return df


# +
# Use case
# connection = connect_to_oracle("my_username", "my_password", "localhost", "1521", "my_service")
# data = fetch_data(connection, "SELECT 1 FROM dual")

# +

def fetch_smb_data(path):
    """
    Function to fetch data from Ozyrys via SMB.

    Args:
        path (str): path to resource on Ozyrys


    Returns:
        DataFrame: Data fetched from Ozyrys.
    """
    with open_file(path, mode = 'rb') as fd:
        df = pd.read_excel(fd)  

    # Result return
    return df


# -
def fetch_livy_data(livy_session, sql, download=False):
    """
    Function to fetch data from Ozyrys via SMB.

    Args:
        path (str): path to resource on Ozyrys


    Returns:
        DataFrame: Data fetched from Ozyrys.
    """
    
    livy_session.run(f'df = spark.sql("{sql}")')
    if download is True:
        df=livy_session.download("df")
        return df
            
    # Result return
    return None



