import pandas as pd
from sqlalchemy import create_engine, MetaData, Table

pd.set_option('display.width', 400)
pd.set_option('display.max_columns', 10)


class DataBase:

    def __init__(self, database, dialect='mysql', driver='pymysql', username='root', password='root', host='127.0.0.1',
                 port="3306"):
        connection_string = (dialect + '+' + driver + '://' + username + ':' + password + '@' + host + ':' + port + "/" + database)
        self.engine = create_engine(connection_string)
        self.df = pd.DataFrame()

    def get_tables(self):
        return self.engine.table_names()

    def get_table_metadata(self, table_name):
        metadata = MetaData()
        return Table(table_name, metadata, autoload=True, autoload_with=self.engine)

    def get_table_into_df(self, query):
        return pd.read_sql(query, self.engine)

    def replicate_rows(self):
        new_df = pd.concat([self.df] * 2, ignore_index=True)
        new_df.set_index('id', inplace=True)
        new_df.reset_index(inplace=True)
        new_df.index += 1
        new_df.index.names = ['new_indexes']
        new_df.drop(columns=['id'], inplace=True)
        new_df.reset_index(inplace=True)
        new_df.rename(columns={'new_indexes': 'id'}, inplace=True)
        new_df.set_index('id', inplace=True)
        return new_df

    def insert_df_into_database_table(self, data_frame, table_name):
        data_frame.to_sql(table_name, con=self.engine, if_exists='append', chunksize=1000)

    def drop_duplicates_from_dataframe(self, data_frame, subset_column):
        return data_frame.drop_duplicates(subset=[subset_column])

    def insert_rows(self, query):
        self.engine.execute(query)











# def connect_database(database, dialect='mysql', driver='pymysql', username='root', password='root', host='127.0.0.1',
#                      port="3306"):
#     connection_string = (
#             dialect + '+' + driver + '://' + username + ':' + password + '@' + host + ':' + port + "/" + database)
#     engine = create_engine(connection_string)
#     engine.connect()
#     return engine
