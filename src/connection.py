# -*- coding: utf-8 -*-
# +
import cx_Oracle

# Oracle connection function
def connect_to_oracle(username, password, host, port, service_name):
    
    """
    Function to connect to the Oracle database.

    Args:
        username (str): Username for the Oracle database.
        password (str): Password for the Oracle database.
        host (str): Host address of the Oracle database.
        port (str): Port of the Oracle database.
        service_name (str): Service name of the Oracle database.

    Returns:
        Connection: Connection to the Oracle database.
    """
    # Create Oracle connection
    con = cx_Oracle.connect(
    user=username,
    password=password,
    dsn=f"{host}:{port}/{service_name}"
    )

    # Other operations related to the connection 
    # ...
    # ...
    # ...

    # Connection return
    return con


# +
# Use case
# connection = connect_to_oracle("my_username", "my_password", "localhost", "1521", "my_service")

# +
from livy import LivySession
from requests_kerberos import HTTPKerberosAuth
import requests
import os

# Livy connection function
def connect_to_livy(livy_url):
    
    """
    Function to connect to Livy.

    Args:
        livy_url (str) : url to Livy .

    Returns:
        Session: Spark Session.
    """
    # Create SMB session
    requests_session = requests.Session()
    requests_session.headers.update(
    {"X-Requested-By": "jhub-user"})

    session = LivySession.create(livy_url, HTTPKerberosAuth(), 
                           requests_session=requests_session)

    # Other operations related to the connection 
    # ...
    # ...
    # ...

    # Session return
    return session
# -


