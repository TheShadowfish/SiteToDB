from pkgutil import get_data

import src.get_data
from src.get_data import Atsenergo, SelAtsenergo


def user_interaction():

    while True:

        what_to_do = input("Program menu: \n"
                           "Get data from site (selenium) (1) \n"
                           "Get data from site (request, bs, pandas) (2) \n"
                           "Write to db (3) \n"
                           "Create daemon to update db once a day (4)"
                           "Exit - any other key \n")



        if what_to_do == '1':
            req_res = SelAtsenergo()
            req_res.get_data()
            # req_res.print_data()

        elif what_to_do == '2':
            req_res = Atsenergo()
            req_res.get_data()

        elif what_to_do == '3':
            print("NotImplemented")

        elif what_to_do == '4':
            print("NotImplemented")
        else:
            exit()



# начало программы
if __name__ == '__main__':
    user_interaction()