from data_loading_2nd_version import *


def main():
    try:
        with PostgresConnection(**DATABASE_CONFIG) as connection:
            uri_constrcuted = uri_construct()
            last_updated_date = fetch_last_updated_timestamp(connection)
            print(last_updated_date)
            df1 = read_data_from_postgres_table('public', 'ride_rating_table', uri_constrcuted, last_updated_date)
            print(df1)
            df_with_load_date = calculate_load_date(df1)
            print(df_with_load_date)
            write_data_to_postgres_table(df_with_load_date, 'public.ride_rating_table_test', uri_constrcuted, 'append')
    except Exception as e:
        print(e)

if __name__=='__main__':
    main()
#
# if __name__ == "__main__":
#     try:
#         uri_constrcuted = uri_construct()
#         #DATABASE_CONFIG-read from file and create this variable
#         connection = PostgresConnection(**DATABASE_CONFIG)
#         connection.connect()#--should be hidden
#         last_updated_date = fetch_last_updated_timestamp(connection)
#         print(last_updated_date)
#         df1 = read_data_from_postgres_table('public','ride_rating_table',uri_constrcuted,last_updated_date)
#         print(df1)
#         write_data_to_postgres_table(df1,'public.ride_rating_table_test',uri_constrcuted,'append')
#     except Exception as e:
#         print(e)


