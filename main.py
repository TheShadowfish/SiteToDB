import datetime
# import os

# import psycopg2
# from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from src.get_data import SelAtsenergo
from src.write_db import DBManager


def user_interaction():

    while True:

        what_to_do = input("Program menu: \n"
                           "Get data from site (selenium) (1) \n"
                           "ReWrite file to DB (2) \n"
                           "Print db (3) \n"
                           "add new data (4) \n"
                           "Create daemon to update db once a day (5) \n"
                           "Exit - any other key \n")

        filepath = "data/my_csv_2024-12-23.csv"

        if what_to_do == "1":
            req_res = SelAtsenergo()
            filepath = req_res.get_data()

        elif what_to_do == "2":
            db_man = DBManager()
            db_man.drop_all()
            if db_man.check_bd_script():
                db_man.drop_all()
            db_man.create_database_script()
            db_man.create_tables_script()

            db_man.write_to_database(filepath)
            db_man.print_database_table(0)

        elif what_to_do == "3":
            db_man = DBManager()

            if db_man.check_bd_script():
                db_man.print_database_table(0)
                db_man.get_data_from_bd("SELECT * FROM statistics;", "Таблица статистики")

                db_man.get_data_from_bd("SELECT max(history_id) FROM statistics;", "Max 'history_id'")

                db_man.get_data_from_bd(
                    f"SELECT * FROM statistics where day='{datetime.date.today().isoformat()}';",
                    f"day='{datetime.date.today().isoformat()}'")

            else:
                print("Database 'astenergo' does not exists or don't have table 'statistics'")

        elif what_to_do == "4":
            db_man = DBManager()
            if db_man.check_bd_script():
                db_man.add_to_database("data/my_csv_2024-12-24.csv")
                # db_man.print_database_table(0)

        elif what_to_do == "5":
            print("NotImplemented")

        else:
            exit()


# начало программы
if __name__ == "__main__":
    user_interaction()
