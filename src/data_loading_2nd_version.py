import polars as pl
import psycopg2
from config import DATABASE_CONFIG,PostgresConnection
connection = PostgresConnection(**DATABASE_CONFIG)
from datetime import datetime


# create a function to construct uri

def uri_construct():
    """
    This function constructs URI to be used for connecting postgres with Polars dataframe
    :return:
    """
    uri_constructed =f"postgresql://{DATABASE_CONFIG['user']}:{DATABASE_CONFIG['password']}@{DATABASE_CONFIG['host']}:{DATABASE_CONFIG['port']}/{DATABASE_CONFIG['database']}"
    return uri_constructed


def fetch_last_updated_timestamp(connection_details):
    query = "select max(updated) from public.ride_rating_table_test"
    result = connection_details.execute_query(query)
    max_updated_datetime = result[0][0]
    max_updated_str = max_updated_datetime.strftime("%Y-%m-%d %H:%M:%S")
    return max_updated_str

def read_data_from_postgres_table(schema_name:str, table_name:str, uri:str, date_value:str)->pl.dataframe:
    """
    This function will read data from postgres table
    :param schema_name:
    :param table_name:
    :param uri:
    :return:
    """
    query = f"select * from {schema_name}.{table_name} where updated >'{date_value}'"
    df = pl.read_database_uri(query,uri)
    return df

def calculate_load_date(df):
    df_new_column = df.with_columns(load_date= datetime.now())
    return df_new_column


def write_data_to_postgres_table(df,table_name, connection_details, exists_mode):
    """
    This will write data into postgres DB
    :param df:
    :param table_name:
    :param connection_details:
    :param exists_mode:
    :return:
    """
    df.write_database(table_name=table_name,connection=connection_details,if_exists =exists_mode)
    return None


