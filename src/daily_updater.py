# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import subprocess

from src.get_data import SelAtsenergo
from src.write_db import DBManager

# crontab -e

# every day
# * * */1 * * /home/drakot/PycharmProjects/SiteToDB/src/daily_updater.py

# every 5 min
# */5 * * * * /home/drakot/PycharmProjects/SiteToDB/src/daily_updater.py

# crontab -l

def sendmessage(message="BD statistics updated/"):
    subprocess.Popen(['notify-send', message])

    req_res = SelAtsenergo()
    filepath = req_res.get_data()

    db_man = DBManager()

    if not db_man.check_bd_script():
        # если базы данных не существует
        db_man.create_database_script()
        db_man.create_tables_script()
        db_man.write_to_database(filepath)
    else:
        db_man.add_to_database(filepath)

        # db_man.print_database_table(0)

    return

if __name__ == '__main__':
    sendmessage()