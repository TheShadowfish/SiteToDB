import csv
from xmlrpc.client import DateTime

import pandas as pd
import requests
import bs4
import lxml
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry


import os
from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium_stealth import stealth
import datetime
import time
from selenium.webdriver.support import expected_conditions as EC


class Atsenergo:

    """Get data from site"""


    def __init__(self):

        self.__url = "https://www.atsenergo.ru/results/rsv/statistics"

        self.resource = None


    def get_data(self):


        # session = requests.Session()
        # retry = Retry(connect=3, backoff_factor=0.5)
        # adapter = HTTPAdapter(max_retries=retry)
        # session.mount('http://', adapter)
        # session.mount('https://', adapter)
        #
        # session.get(url)
        # context = ssl._create_unverified_context()


        source = requests.get(self.__url, verify=False)

        print(source.text)

        # soup = BeautifulSoup(s, "html.parser")

        soup = bs4.BeautifulSoup(source, "html.parser")
        # table = soup.select_one('.historical_data_table')

        table = soup.select_one('.info-table')

        print(table)

        historical_data = pd.read_html(str(table))

        print(historical_data)

        historical_data = historical_data[0]

        historical_data.to_csv('mcd_income.csv',
                               index=False)



class SelAtsenergo:

    def __init__(self):

        self.__url = "http://www.atsenergo.ru/results/rsv/statistics"

        # self.__url = 'https://www.example.com'




    @staticmethod
    def get_random_chrome_user_agent():
        user_agent = UserAgent(browsers='chrome', os='windows', platforms='pc')
        return user_agent.random

    @staticmethod
    def create_driver(user_id=1):
        options = Options()
        options.add_argument("start-maximized")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        script_dir = os.path.dirname(os.path.abspath(__file__))
        base_directory = os.path.join(script_dir, 'users')
        user_directory = os.path.join(base_directory, f'user_{user_id}')

        options.add_argument(f'user-data-dir={user_directory}')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument("--disable-notifications")
        options.add_argument("--disable-popup-blocking")
        options.add_argument('--no-sandbox')
        # options.add_argument('--headless')

        driver = webdriver.Chrome(options=options)
        ua = SelAtsenergo.get_random_chrome_user_agent()
        stealth(driver=driver,
                user_agent=ua,
                languages=["ru-RU", "ru"],
                vendor="Google Inc.",
                platform="Win32",
                webgl_vendor="Intel Inc.",
                renderer="Intel Iris OpenGL Engine",
                fix_hairline=True,
                run_on_insecure_origins=True
                )

        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            'source': '''
                delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
                delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
                delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
          '''
        })
        return driver




    def get_data(self, user_id=1):

        driver = self.create_driver(user_id)
        driver.get(self.__url)

        table = driver.find_element(By.ID, "aid_stats_table")
        print(table.text)
        print("-----------------------")


        t_data = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.ID, "aid_stats_table"))).get_attribute("outerHTML")
        df = pd.read_html(t_data)


        current_date = datetime.date.today().isoformat()

        # and now we get this table
        df[0].to_csv(f"data/my_csv_{current_date}.csv", index=False)

        print(df)
        print("-----------------------")
        driver.quit()


