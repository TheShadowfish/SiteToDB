import os

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from src.get_data import SelAtsenergo
from src.write_db import DBManager


def user_interaction():

    while True:

        what_to_do = input("Program menu: \n"
                           "Get data from site (selenium) (1) \n"
                           "ReWrite file to DB (2) \n"
                           "Print db (3) \n"
                           "DROP ALL !!! (4) \n"
                           "Create daemon to update db once a day (5) \n"
                           "Exit - any other key \n")

        filepath = "data/my_csv_2024-12-23.csv"

        if what_to_do == "1":
            req_res = SelAtsenergo()
            filepath = req_res.get_data()

        elif what_to_do == "2":
            DBman = DBManager()

            DBman.write_to_database(filepath)

            DBman.print_database_table(0)


        elif what_to_do == "3":
            DBman = DBManager()

            DBman.print_database_table(0)

            DBman.get_data_from_bd("SELECT * FROM statistics;", "Таблица статистики")

        elif what_to_do == "4":
            drop_all()

        elif what_to_do == "5":
            print("NotImplemented")

        else:
            exit()

def drop_all():
    postgre_params = {
        "host": os.getenv('DB_POSTRESQL_HOST'),
        "database": "postgres",
        "user": os.getenv('DB_POSTRESQL_USER'),
        "password": os.getenv('DB_POSTRESQL_PASSWORD')
    }

    con = psycopg2.connect(**postgre_params)
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    cur.execute(f"DROP DATABASE {os.getenv('DB_POSTRESQL_DB_NAME')};")
    cur.close()
    con.close()
    print(f"{os.getenv('DB_POSTRESQL_DB_NAME')}: база дропнута.! ")



# начало программы
if __name__ == "__main__":
    user_interaction()
