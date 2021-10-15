from main import *
import pandas as pd
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=UserWarning)

obj = DataBase(database='devnation_2021')
print(obj.get_tables())

print(repr(obj.get_table_metadata(table_name='user_details')))

df = obj.get_table_into_df('Select * from user_details LIMIT 10')
print(df)

new_df = obj.replicate_rows()
print(new_df)

obj.insert_df_into_database_table(new_df,table_name='analysis_abc')

unique_df = obj.drop_duplicates_from_dataframe(new_df, 'user_id')
obj.insert_df_into_database_table(unique_df, 'unique_user_details')

obj.insert_rows("""INSERT INTO user_details_credentials (user_id, password)
VALUES (2, 'Tom3');""")

