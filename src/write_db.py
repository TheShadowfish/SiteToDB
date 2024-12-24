import os

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from dotenv import load_dotenv

load_dotenv()

class DBManager:
    """Связь с базой данных"""

    def __init__(self):
        self.file_extension = "PostgresSQL"
        self.__db_name = os.getenv("DB_POSTRESQL_DB_NAME")
        self.__tables = ["statistics",]
        self.__columns_string = "day, price_zone, PCB_index, percent_change, planed_volume_consumption, \
                                    percent_change_2, planed_volume_prod_TES, planed_volume_prod_GES, \
                                    planed_volume_prod_AES, planed_volume_prod_SES_VES"

        self.conn_params = {
            "host": os.getenv("DB_POSTRESQL_HOST"),
            "database": self.__db_name,
            "user": os.getenv("DB_POSTRESQL_USER"),
            "password": os.getenv("DB_POSTRESQL_PASSWORD")
        }

    def check_bd_script(self):
        "Check database exists (False/True)"
        try:
            with psycopg2.connect(**self.conn_params) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM statistics")
            conn.close()
        except Exception as e:
            print(f"Исключение {e}. База данных {self.conn_params} и таблицы в ней еще не созданы")
            return False
        else:
            return True

    def create_database_script(self):
        """Create database"""
        postgre_params = {
            "host": os.getenv("DB_POSTRESQL_HOST"),
            "database": "postgres",
            "user": os.getenv("DB_POSTRESQL_USER"),
            "password": os.getenv("DB_POSTRESQL_PASSWORD")}

        con = psycopg2.connect(**postgre_params)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()

        cur.execute(f"CREATE DATABASE {self.__db_name};")
        cur.close()
        con.close()
        print(f"{self.__db_name} создана!")

    def create_tables_script(self):
        """Create tables for statistics_DB"""

        create_statistics = "CREATE TABLE statistics ( \
                                    history_id SERIAL PRIMARY KEY, \
                                    day date NOT NULL, \
                                    price_zone int  NOT NULL, \
                                    PCB_index float not null, \
                                    percent_change varchar(8) NOT NULL, \
                                    planed_volume_consumption float NOT NULL, \
                                    percent_change_2 varchar(8) NOT NULL, \
                                    planed_volume_prod_TES float NOT NULL, \
                                    planed_volume_prod_GES float NOT NULL, \
                                    planed_volume_prod_AES float NOT NULL, \
                                    planed_volume_prod_SES_VES float NOT NULL \
                                    );"

        #  correct_tuple_list = [("День", "Ценовая зона",	"Индекс РСВ на покупку руб/МВт*ч", "% изм",
        #                   "Объем полного планового потребления МВт*ч",
        #                   "% изм1", "Объем планового производства по типам станций МВт*ч ТЭС",
        #                   "Объем планового производства по типам станций МВт*ч ГЭС",
        #                   "Объем планового производства по типам станций МВт*ч АЭС",
        #                   "Объем планового производства по типам станций МВт*ч СЭС/ВЭС"),]
        # ('23.11.2024', '2', '1991.87', '+3.49%', '698681.539',
        # '+0.80%', '414180.837', '277239.000', '0.000', '330.103')

        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(create_statistics)
                    print("Таблица statistics создана!")
                except Exception as e:
                    print(f"Исключение при создании таблицы  БД: {e}. Запрос {create_statistics} не пашет")
        conn.close()

    def add_or_rewrite_db(self, csv_file_path: str, rewrite=False):

        if not os.path.exists(csv_file_path):
            print(f"Данные по адресу {csv_file_path} отсутствуют.")
            return

        if rewrite:
            # clear old data to write new
            self.clear_all_tables()

            tuple_string_list = DBManager.get_tuple_list_from_file(csv_file_path)
            if len(tuple_string_list) < 1:
                print(f"File {csv_file_path} is empty!")
                return

        else:

            unfiltered_string_list = DBManager.get_tuple_list_from_file(csv_file_path)
            # string_s = ", ".join(["%s" for i in range(len(tuple_string_list[0]))])

            tuple_string_list = []
            for t in unfiltered_string_list:
                if not self.check_in_db_table(t[1], t[2]):
                    tuple_string_list.append(t)

            if len(tuple_string_list) < 1:
                print("No new data!")
                return
            else:
                print("add new data:")
                [print(s) for s in tuple_string_list]

        string_s = ", ".join(["%s" for i in range(len(tuple_string_list[0]))])

        conn2 = psycopg2.connect(**self.conn_params)
        cur = conn2.cursor()

        try:
            cur.executemany(f"INSERT INTO {self.__tables[0]}({self.__columns_string}) VALUES ({string_s})", tuple_string_list)
            # cur.executemany(f"INSERT INTO {self.__tables[0]} VALUES ({string_s})", tuple_string_list)
        except Exception as e:
            print(f"\n ОШИБКА: {e} при записи следующего:")
            print(f"INSERT INTO {self.__tables[0]} VALUES ({self.__columns_string}) {tuple_string_list}")
            input("Ошибка, ознакомьтесь! Программа продолжит работу после нажатия Enter. \n")
        else:
            # если запрос без ошибок - заносим в БД
            conn2.commit()
        finally:
            cur.close()
            conn2.close()


    def write_to_database(self, csv_file_path: str):
        self.add_or_rewrite_db(csv_file_path, True)
        # if not os.path.exists(csv_file_path):
        #     print(f"Данные по адресу {csv_file_path} отсутствуют.")
        #     return
        #
        # tuple_string_list = DBManager.get_tuple_list_from_file(csv_file_path)
        # string_s = ", ".join(["%s" for i in range(len(tuple_string_list[0]))])
        #
        # conn2 = psycopg2.connect(**self.conn_params)
        # cur = conn2.cursor()
        # #
        # # INSERT INTO student(first_name, last_name, birth_date, phone)
        # # VALUES
        # # ('Иванов', 'Петр', '01.04.1996', '8-999-876-54-32'),
        # # ('Петров', 'Иван', '11.05.1994', '8-988-876-54-31'),
        # # ('Сидорова', 'Евгения', '21.05.1994', '8-987-876-54-31');
        #
        # try:
        #     cur.executemany(f"INSERT INTO {self.__tables[0]}({self.__columns_string}) VALUES ({string_s})", tuple_string_list)
        #     # cur.executemany(f"INSERT INTO {self.__tables[0]} VALUES ({string_s})", tuple_string_list)
        # except Exception as e:
        #     print(f"\n ОШИБКА: {e} при записи следующего:")
        #     print(f"INSERT INTO {self.__tables[0]} VALUES ({self.__columns_string}) {tuple_string_list}")
        #     input("Ошибка, ознакомьтесь! Программа продолжит работу после нажатия Enter. \n")
        # else:
        #     # если запрос без ошибок - заносим в БД
        #     conn2.commit()
        # finally:
        #     cur.close()
        #     conn2.close()

    def add_to_database(self, csv_file_path: str):
        self.add_or_rewrite_db(csv_file_path, False)


    def check_in_db_table(self, day, price_zone):
        """Check if this string in database yet (False/True)"""

        request = f"SELECT count(*) FROM statistics where day='{day}' and price_zone={price_zone};"
        # is_yet = True

        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute(request)
                rows = cur.fetchall()
                if int(rows[0][0]) > 0:
                    is_yet = True
                else:
                    print(f"...FOUND NEW DATA!: {day}, {price_zone}")
                    is_yet = False
        conn.close()
        return is_yet

    @staticmethod
    def drop_all():
        postgre_params = {
            "host": os.getenv("DB_POSTRESQL_HOST"),
            "database": "postgres",
            "user": os.getenv("DB_POSTRESQL_USER"),
            "password": os.getenv("DB_POSTRESQL_PASSWORD")
        }
        con = psycopg2.connect(**postgre_params)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()

        cur.execute(f"DROP DATABASE {os.getenv('DB_POSTRESQL_DB_NAME')};")
        cur.close()
        con.close()
        print(f"{os.getenv('DB_POSTRESQL_DB_NAME')}: база дропнута.! ")

    @staticmethod
    def get_tuple_list_from_file(filepath):
        """Lines from cvs file to tuple list"""
        correct_tuple_list = []
        with open(filepath, "rt") as my_txt:
            counter = 0
            for data_line in my_txt:
                if counter > 1:
                    # next_line = str(counter) + ", " + data_line.strip("\r""\n").replace(" ", "")
                    next_line = data_line.strip("\r""\n").replace(" ", "")
                    next_tuple = tuple(next_line.split(","))
                    correct_tuple_list.append(next_tuple)
                counter += 1
            return correct_tuple_list

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

    def clear_all_tables(self):
        print("clear_all_tables")
        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE statistics RESTART IDENTITY CASCADE;")
                print("Таблица statistics удалена!")

        conn.close()
