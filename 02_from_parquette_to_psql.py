# загрузка обработанных данных из parquette в psql
import pandas as pd
from sqlalchemy import create_engine
import os
 
password_str= input ('Ведите пароль для подключения к базе данных')
def create_db_connection(sql_password):
    # Параметры подключения к PostgreSQL
    db_params = {
    'host': 'localhost',
    'port': "5432",
    'user': 'sa',
    'password': sql_password,
    'database': 'Rosreestr'
    }
    # Создание строки подключения к базе данных PostgreSQL
    engine_str = f'postgresql+psycopg2://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}'
    engine = create_engine(engine_str)
    return engine
def from_parquette_to_psql (parquette_file_path,sql_table_name,db_connection):
    df_data = pd.read_parquet(parquette_file_path)
    df_data.to_sql(sql_table_name, db_connection, index=False, if_exists='append')
#------------------------------------------------------------
def get_all_files(folder_path):
    files_list = []
    # Рекурсивная функция для обхода всех файлов в папке и подпапках
    def recursive_list_files(folder):
        for root, dirs, files in os.walk(folder):
            for file in files:
                file_path = os.path.join(root, file)
                if os.path.isfile(file_path):
                    files_list.append(file_path)
    recursive_list_files(folder_path)
    return files_list
#------------------------------------------------------------

folder_path = r"c:\Pet_project\Output\parquette"
all_files = get_all_files(folder_path)

db_connect=create_db_connection(password_str)
for element in all_files:
    from_parquette_to_psql (element,'svod_all_2',db_connect)
    print(element)
