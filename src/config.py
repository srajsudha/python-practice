# config.py
import psycopg2

class PostgresConnection:
    def __init__(self, host, user, password, database, port):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.connection = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()



    def connect(self):
        if not self.connection:
            connection_parms = {
                'host':self.host,
                'user':self.user,
                'password':self.password,
                'database':self.database,
                'port':self.port
            }
            self.connection = psycopg2.connect(**connection_parms)

    def disconnect(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    def execute_query(self,query):
        if not self.connection:
            raise ValueError("Not connected to database")
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()

        return result


DATABASE_CONFIG = {
    'host': 'rajpostgresserver.postgres.database.azure.com',
    'user': 'rajpracticepostgres',
    'password': 'Madhu1001',
    'database': 'practice',
    'port': '5432',
}

# move this config in a file
connection = PostgresConnection(**DATABASE_CONFIG)