import psycopg2
import polars as pl
from config import DATABASE_CONFIG


# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(**DATABASE_CONFIG)
cursor = conn.cursor()
sql_query = "SELECT MAX(updated) FROM public.ride_rating_table_test"
cursor.execute(sql_query)
max_updated_timestamp = cursor.fetchone()[0]
max_updated_timestamp_str = str(max_updated_timestamp)
print(max_updated_timestamp_str)
# Print the DataFrame
# print(df)

# conn1 = pl.Connection("postgresql://rajpracticepostgres:Madhu1001@rajpostgresserver.postgres.database.azure.com:5432/practice")

query = f"select * from public.ride_rating_table where updated >'{max_updated_timestamp_str}'"

# print(query)

df = pl.read_database_uri(query,uri ="postgresql://rajpracticepostgres:Madhu1001@rajpostgresserver.postgres.database.azure.com:5432/practice")
print(df)
# result = pl.read_database_uri(query,"postgresql://rajpracticepostgres:Madhu1001@rajpostgresserver.postgres.database.azure.com:5432/practice")

#df.write_database('public.ride_rating_table_test',connection = "postgresql://rajpracticepostgres:Madhu1001@rajpostgresserver.postgres.database.azure.com:5432/practice" ,if_exists='append')


