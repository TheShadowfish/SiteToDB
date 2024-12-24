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
                           "Drop and create DB (2) \n"
                           "Rewrite table from file (3) \n"
                           "Add data from file (4) \n"          
                           "Print db (5) \n"
                           "Create daemon to update db once a day (6) \n"
                           "Exit - any other key \n")

        filepath = "data/my_csv_2024-12-22.csv"
        db_man = DBManager()

        if what_to_do == "1":
            req_res = SelAtsenergo()
            filepath = req_res.get_data()

        elif what_to_do == "2":
            try:
                db_man.drop_all()
            except Exception as e:
                print(e)
            # if db_man.check_bd_script():
            #     db_man.drop_all()
            db_man.create_database_script()
            db_man.create_tables_script()

            db_man.write_to_database(filepath)
            db_man.print_database_table(0)

        elif what_to_do == "3":
            if db_man.check_bd_script():
                db_man.write_to_database(filepath)
            else:
                print("Database 'astenergo' does not exists or don't have table 'statistics'")

        elif what_to_do == "4":
            filepath = "data/my_csv_2024-12-24.csv"
            if db_man.check_bd_script():
                db_man.add_to_database(filepath)
            else:
                print("Database 'astenergo' does not exists or don't have table 'statistics'")

        elif what_to_do == "5":

            if db_man.check_bd_script():
                db_man.print_database_table(0)
                db_man.get_data_from_bd("SELECT * FROM statistics;", "Таблица статистики")

                db_man.get_data_from_bd("SELECT max(history_id) FROM statistics;", "Max 'history_id'")

                db_man.get_data_from_bd(
                    f"SELECT * FROM statistics where day='{datetime.date.today().isoformat()}';",
                    f"day='{datetime.date.today().isoformat()}'")

            else:
                print("Database 'astenergo' does not exists or don't have table 'statistics'")

        elif what_to_do == "6":
            req_res = SelAtsenergo()
            filepath = req_res.get_data()

            if not db_man.check_bd_script():
            # если базы данных не существует
                db_man.create_database_script()
                db_man.create_tables_script()
                db_man.write_to_database(filepath)
            else:
                db_man.add_to_database(filepath)

                db_man.print_database_table(0)
        else:
            exit()


# начало программы
if __name__ == "__main__":
    user_interaction()
