import cx_Oracle
import pandas as pd
import numpy as np

def save_to_table(connection, df, table_name, overwrite=True, create_if_not_exist=False):
    """
    Function to write DataFrame to the Oracle database.

    Args:
        connection (cx_Oracle.Connection): Connection to the Oracle database.
        df (pd.DataFrame): DataFrame to be written to the database.
        table_name (str): Name of the table to write data to.
        overwrite (bool): Whether to overwrite the table if it already exists.
        create_if_not_exist (bool) : Whether to create new table
    """
    cursor = connection.cursor()


    if  overwrite:
        try:
            cursor.execute(f"TRUNCATE TABLE {table_name}")
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            if error.code == 942:
                pass
            else:
                raise(e)
    
    if create_if_not_exist:
        # Create table statement
        create_table_query = f"CREATE TABLE {table_name} ("
        for col_name, dtype in zip(df.columns, df.dtypes):
            if dtype == 'int64':
                col_type = 'NUMBER'
            elif dtype == 'float64':
                col_type = 'FLOAT'
            elif dtype == 'object':
                col_type = 'VARCHAR2(4000)'
            elif dtype == 'datetime64[ns]':
                col_type = 'TIMESTAMP'
            else:
                col_type = 'VARCHAR2(4000)'  # Default for other types

            create_table_query += f"{col_name} {col_type}, "

        create_table_query = create_table_query.rstrip(", ") + ")"
        
        try:
            cursor.execute(create_table_query)
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            if error.code == 955:
                pass
            else:
                raise(e)
    
    # Replace NaN with None
    df = df.replace(np.nan, None)
    df = df.replace('NaT', None)


    # Convert DataFrame rows to list of tuples, ensuring types are Python-native
    data = []
    for row in df.itertuples(index=False, name=None):
        processed_row = []
        for value in row:
            if isinstance(value, pd.Timestamp):
                processed_row.append(value.strftime('%Y-%m-%d %H:%M:%S'))
            elif isinstance(value, (np.generic, np.ndarray)):
                processed_row.append(value.item())
            else:
                processed_row.append(value)
        data.append(tuple(processed_row))
    
   # Construct the SQL query to insert data
    columns = ', '.join(df.columns)
    values = []
    for i, dtype in enumerate(df.dtypes):
        if dtype == 'datetime64[ns]':
            values.append(f"TO_DATE(:{i+1}, 'YYYY-MM-DD HH24:MI:SS')")
        else:
            values.append(f":{i+1}")
    values = ', '.join(values)
    sql_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
    # Execute many for better performance
    cursor.executemany(sql_query, data)

    
    connection.commit()
    cursor.close()

# Example usage
# connection = cx_Oracle.connect(user="username", password="password", dsn="dsn_string")
# df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c'], 'col3': [pd.Timestamp('2010-02-03 21:18:31'), pd.Timestamp('2010-02-03 21:18:27'), pd.Timestamp('2010-02-03 21:18:34')]})
# save_to_table(connection, df, "my_table", overwrite=True)


def send_query(connection, statement):
    """
    Function to execute query on the Oracle database.

    Args:
        connection (Connection): Connection to the Oracle database.
        statement (str): SQL statement to execute.
    """
    cursor = connection.cursor()
    cursor.execute(statement)
    connection.commit()
    cursor.close()

    # Other operations related to executing insert
    # ...
    # ...
    # ...



def execute_stored_procedure(connection, procedure_statement):
    """
    Function to execute a stored procedure on the Oracle database.

    Args:
        connection (Connection): Connection to the Oracle database.
        procedure_statement (str): Statement to execute the stored procedure.
    """
    cursor = connection.cursor()
    cursor.callproc(procedure_statement)
    cursor.close()

    # Other operations related to executing oracle procedure
    # ...
    # ...
    # ...
