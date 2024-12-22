import csv
import os

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# from decouple import config
from dotenv import load_dotenv

load_dotenv()


# from abc import ABC, abstractmethod


class DBManager():
    """
    Связь с базой данных
    """

    def __init__(self):
        self.file_extension = 'PostgresSQL'
        self.__db_name = os.getenv('DB_POSTRESQL_DB_NAME'),
        self.__tables = ('statistics')

        self.conn_params = {
            "host": os.getenv('DB_POSTRESQL_USER'),
            "database": self.__db_name,
            "user": os.getenv('DB_POSTRESQL_HOST'),
            "password": os.getenv('DB_POSTRESQL_PASSWORD')}

        self.check_bd_script()

    def check_bd_script(self):
        """
        Create database if not exist
        """
        try:
            with psycopg2.connect(**self.conn_params) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM statistics")
            conn.close()
        except Exception as e:
            print(f'Исключение {e}. База данных {self.conn_params} и таблицы в ней еще не созданы')
            if input('Создать БД автоматически? Без этого дальнейшая работа будет невозможна. Y/N') != 'Y':
                exit(1)
            else:
                print(f'create {self.__db_name}...')
                self.create_statistics_db()
                print(f'generate tables {self.__tables}...')
                self.generate_bd_script()
        else:
            print(f'База данных {self.conn_params} уже существует. Продолжаем работу.')

    def create_statistics_db(self):
        """Create database if not exists"""
        postgre_params = {
            "host": os.getenv('DB_POSTRESQL_USER'),
            "database": "postgres",
            "user": os.getenv('DB_POSTRESQL_HOST'),
            "password": os.getenv('DB_POSTRESQL_PASSWORD')}

        con = psycopg2.connect(**postgre_params)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()

        cur.execute(f'CREATE DATABASE {self.__db_name};')
        cur.close()
        con.close()
        print(f"{self.__db_name} создана! ")

    def clear_all_tables(self):
        print("clear_all_tables")
        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute('TRUNCATE TABLE statistics RESTART IDENTITY CASCADE;')
                print("Таблица statistics удалена!")
        conn.close()

    def generate_bd_script(self):
        """
        Create tables for statistics_DB
        """

        create_statistics = "CREATE TABLE statistics (\
                                    employer_id int UNIQUE PRIMARY KEY,\
                                    name varchar(200) NOT NULL,\
                                    url varchar(200) NOT NULL,\
                                    vacancies_url varchar(250) NOT NULL\
                                    );"


        print("Подключение...")
        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute(create_statistics)
                print("Таблица statistics создана!")

        conn.close()

    def print_database_table(self, table_number=0):
        if len(self.__tables) < table_number < len(self.__tables):
            raise ValueError(f"В базе только {len(self.__tables)} таблицы: {self.__tables}.")
        print(f"\n Таблица в базе {self.__db_name}(localhost) {self.__tables[table_number]}: \n")
        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {self.__tables[table_number]}")
                rows = cur.fetchall()
                for row in rows:
                    print(row)
        conn.close()

    def write_to_database(self, csv_file_path: str, append_only_new_data=False):

        if not os.path.exists(csv_file_path):
            print(f"Данные по адресу {csv_file_path} отсутствуют.")
            return
        #     parent_dir = os.path.dirname(os.path.abspath(__file__))
        #     filepath = os.path.join(parent_dir, filename)
        # return filepath

        data_csv = []

        with open(csv_file_path) as my_csv:
            reader = csv.DictReader(my_csv)
            data_csv = list(reader)


            data_csv.pop([0])


        # количество параметров VALUES в INSERT INTO, т.е. (%s, %s, ... %s)
        string_s = ', '.join(['%s' for i in range(len(data_csv[0]))])

        # превращает числовые значения в числа, строковые не меняет
        int_from_str = lambda x: int(x) if x.isdigit() else x
        # список кортежей для cur.executemany
        tuple_string = [tuple([int_from_str(v) for v in line.values()]) for line in data_csv]

        conn2 = psycopg2.connect(**self.conn_params)
        cur = conn2.cursor()
        # print(f"INSERT INTO {tablename} VALUES ({string_s}) {tuple_string}")

        try:
            cur.executemany(f"INSERT INTO {self.__tables[0]} VALUES ({string_s})", tuple_string)
        except Exception as e:
            print(f'Ошибка: {e}')
        else:
            # если запрос без ошибок - заносим в БД
            conn2.commit()
        finally:
            cur.close()
            conn2.close()


    def get_data_from_bd(self, request, request_name):
        """
        Получает данные с помощью запроса
        Потом это выводится на печать
        request_name - что запрос должен показать
        """
        print(f"ЗАПРОС: {request_name}:")
        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute(request)
                rows = cur.fetchall()
                for row in rows:
                    print(row)
        conn.close()

