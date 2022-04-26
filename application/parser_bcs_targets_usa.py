import requests
from bs4 import BeautifulSoup
from portfolio_package import sqlite_modul as sqm
from portfolio_package import analyst_modul as anm
import configparser
import os
import logging
from time import sleep
from datetime import datetime, time
from dateutil.parser import parse
import random

name_program = 'parser_bcs_targets_usa'
comment_prog = 'получение таргетов акций для американского рынка с сайта bcs-express.ru'

path_config_file = 'settings_expert_system.ini'   # файл с конфигурацией

# user-agent берется из свойств браузера через консоль
name_user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36'
headers = {'user-agent': name_user_agent, 'accept': '*/*'}

# база данных
# путь к БД
path = 'C:\DB_TEST'
name_db = 'usa_market_test.db'
db_name = path + '\\' + name_db

# имя сводной таблицы
name_main_table = '_Tickers'

# индекс тикера с которого начинать загрузку (=1 - первый тикер в сводной таблице)
index_ticker = 1


# функция для загрузки файла конфигурации
def crud_config(path_):
    if not os.path.exists(path_):
        path_system = 'C:\Windows\System32' + '\\' + path_
        print("Не обнаружен файл конфигурации в ", path_, ' смотрим в ', path_system)
        logging.warning("Не обнаружен файл конфигурации в %s смотрим в %s_1", path_, path_system)
        path_ = path_system
        if not os.path.exists(path_):
            print("Не обнаружен файл конфигурации и в ", path_)
            logging.warning("Не обнаружен файл конфигурации и в %s", path_)

    config = configparser.ConfigParser()
    config.read(path_)

    # Читаем значения из конфиг. файла и присваиваем их глобальным переменным
    global path
    global name_db
    global name_main_table
    global index_ticker

    # парсер используется только для базы usd, поэтому сразу выбираем нужный путь из config-файла
    path = config.get("Settings", "path")
    name_db = config.get("Settings", "name_db")
    name_main_table = config.get("Settings", "name_main_table")
    index_ticker = int(config.get("Settings", "index_ticker"))


def target_trend_modul(conn_obj, curs_obj, ticker: str, info_tuple: tuple):
    # проверим существование таблицы
    name_table_target = ticker + '_Target_trend' + '_bcs'
    if sql_obj.check_table_is_exists(curs_obj, name_table_target):
        print("Таблица ", name_table_target, " существует")
        logging.info('Таблица %s существует', name_table_target)
    else:
        # таблицы не существует. создадим ее
        logging.warning('Таблица %s не обнаружена. Создадим ее', name_table_target)
        q = """ CREATE TABLE {table}
                                    (
                                    Ticker text NOT NULL,
                                    Date_update date DEFAULT '1970.01.01',
                                    Period date DEFAULT '1970.01',
                                    Target real DEFAULT 0,
                                    Change real DEFAULT 0,
                                    Change_str text DEFAULT ''
                                    )
                                    """
        q_mod = q.format(table=name_table_target)
        # print(q_mod)
        curs_obj.execute(q_mod)
        conn_obj.commit()

    if sql_obj.check_table_is_exists(curs_obj, name_table_target):
        current_date = datetime.now()
        # истории таргетов нет, есть только текущая цена
        # датой таргета будет текущий месяц
        target_0m = info_tuple[1]
        target_date = datetime(current_date.year, current_date.month, 1, 0, 0, 0)
        r_time = time(0, 0)

        if target_0m >= 0:
            data_load = (ticker,
                         sql_obj.current_date(),
                         target_date,
                         target_0m,
                         0,
                         ''
                         )

            # если нет строки с соответствующей датой, добавляем ее
            if sql_obj.select_date_time_(curs_obj, name_table_target, 'Period', target_date, r_time) is None:
                # print('Строки с указанной датой еще нет')
                q = "INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?)"
                q_ = q.format(table=name_table_target)
                curs_obj.execute(q_, data_load)
                conn_obj.commit()
            else:
                # print('Строка с указанной датой уже есть')
                # удаляем старую строку, добавляем новую
                date_condition = datetime.combine(target_date, r_time)
                sql_obj.delete_rows_condition(conn_obj, curs_obj, name_table_target, 'Period', date_condition)
                q = "INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?)"
                q_ = q.format(table=name_table_target)
                curs_obj.execute(q_, data_load)
                conn_obj.commit()

            # следующий шаг прописать изменение таргета
            # загружаем столбцы с таргетом и датой и идем по ним снизу вверх
            q = '''SELECT {column}, {column_1} FROM {table}'''
            q_ = q.format(table=name_table_target, column='Target', column_1='Period')
            curs_obj.execute(q_)
            list_target = curs_obj.fetchall()
            # [(2.0, '2021-06-01 00:00:00'), (2.1, '2021-07-01 00:00:00'), (2.1, '2021-08-01 00:00:00'),
            # (2.2, '2021-09-01 00:00:00')]
            # запускаем цикл справа налево
            size_frame = len(list_target)
            if size_frame > 1:
                for i in range(size_frame - 1, 0, -1):
                    # если предыдущее значение существует
                    if (i - 1) >= 0:
                        # изменение таргета
                        if list_target[i][0] > 0 and list_target[i - 1][0] > 0:
                            change_raiting = list_target[i][0] - list_target[i - 1][0]
                            change_raiting_str = ''
                            if change_raiting < 0:
                                change_raiting_str = 'Down'
                            if change_raiting > 0:
                                change_raiting_str = 'Up'
                            # заносим данные в таблицу
                            sql_obj.update_data_in_cell(conn_obj, curs_obj, name_table_target,
                                                        'Change', str(change_raiting), 'Period',
                                                        list_target[i][1])
                            sql_obj.update_date_in_cell(conn_obj, curs_obj, name_table_target,
                                                        'Change_str', change_raiting_str, 'Period',
                                                        list_target[i][1])


####################################################################################################
logging.basicConfig(filename='parser_bcs_targets_usa.log',
                    format='[%(asctime)s] [%(levelname)s] => %(message)s',
                    filemode='w',
                    level=logging.DEBUG)

# загрузка параметров из файла конфигурации
crud_config(path_config_file)
print('Load settings from configuration files: ')
print('path: ', path)
print('name_db : ', name_db)
print('name_main_table : ', name_main_table)
print('index_ticker : ', index_ticker)

filename = path + '\\' + name_db
print('Используется БД: ', filename)
logging.info('Используется БД: %s', filename)

sql_obj = sqm.SQLite()

# проверка наличия БД
if sql_obj.check_db(db_name) is False:
    print("База данных ", db_name, " не обнаружена")
    logging.warning('База данных %s не обнаружена', filename)
    quit()
else:
    print("База данных ", db_name, " существует")
    logging.info('База данных %s существует', filename)

    # устанавливаем соединение и создаем обьект курсора
    con = sql_obj.create_connection(db_name)
    cursorObj = con.cursor()

    # проверим существование системных таблиц
    control_table_sys_log = sql_obj.check_table_is_exists(cursorObj, '_System_log')
    if control_table_sys_log is False:
        print('Таблицы _System_log не существует. Нужно создать ее вручную')
        logging.warning('Таблицы %s не существует. Нужно создать ее вручную', '_System_log')
    if control_table_sys_log is False:
        con.close()
        quit()

    ticker_list = []
    recommendation_list = []
    target_list = []
    url_page = 'https://bcs-express.ru/targets?type=american'
    r = requests.get(url_page, headers=headers, params=None)
    if r.status_code == 200:
        # получен ответ сервера
        print('Ответ сервера для тикера для страницы ', url_page, ' ', r.status_code)
        logging.info('Ответ сервера для страницы %s %d', url_page, r.status_code)
        soup = BeautifulSoup(r.text, 'html.parser')

        # названия тикеров в таблице "Консенсус-прогнозы инвестдомов"
        items_tickers = soup.find_all('a', class_='targets-table__title')
        # [<a class ="targets-table__title" href="/kotirovki-i-grafiki/okeylondon" > Okey Gr (LSE) < / a >,
        # < a class ="targets-table__title" href="/kotirovki-i-grafiki/qiwi" > QIWI < / a >,
        for item in items_tickers:
            s = item.get_text()
            s_1 = s.rstrip()
            ticker_list.append(s_1)

        # рекоммендация
        items_recommendation = soup.find_all('div', class_='targets-table__cell _recommend')
        # [<div class="targets-table__cell _recommend"><span class="targets-table__cell-text">Рекомендации</span></div>,
        #  <div class="targets-table__cell _recommend"><span class="targets-table__cell-text">
        #  <span class="targets-table__recommend _sell">Продавать</span></span></div>,
        #  <div class="targets-table__cell _recommend"><span class="targets-table__cell-text">
        #  <span class="targets-table__recommend _keep">Держать</span></span></div>,
        #  <div class="targets-table__cell _recommend"><span class="targets-table__cell-text">
        #  <span class="targets-table__recommend _buy">Покупать</span></span></div>,
        for item in items_recommendation:
            s = item.get_text()
            # заменяем спецсимволы пробелами
            s_1 = s.replace('\n', ' ')
            # удаляем все пробелы
            s_2 = s_1.strip()
            recommendation_list.append(s_2)

        # таргет
        items_target = soup.find_all('div', class_='targets-table__cell _target')
        for item in items_target:
            s = item.get_text()
            # заменяем спецсимволы пробелами
            s_1 = s.replace('\n', ' ')
            # удаляем все пробелы
            s_2 = s_1.strip()
            # заменяем ',' на '.'
            s_3 = s_2.replace(',', '.')
            target_list.append(s_3)

        # print(len(ticker_list))
        # print(ticker_list)

        # удаляем 1-ый индекс, в котором заголовок
        del recommendation_list[0]
        # print(len(recommendation_list))
        # print(recommendation_list)

        # удаляем 1-ый индекс, в котором заголовок
        del target_list[0]
        # print(len(target_list))
        # print(target_list)

        # три списка должны иметь одинаковую длину
        if len(ticker_list) == len(recommendation_list) and len(ticker_list) == len(target_list):
            pass
        else:
            print('Итоговые списки отличаются по длине ', len(ticker_list), ' ', len(recommendation_list),
                  ' ', len(target_list))
            logging.warning('Итоговые списки отличаются по длине %d, %d, %d', len(ticker_list),
                            len(recommendation_list), len(target_list))
            sql_obj.write_event_in_system_log(sql_obj.current_date(), name_program, comment_prog, 1)
            con.close()
            quit()
    else:
        # страница не отвечает
        print('Ответ сервера для тикера для страницы ', url_page, ' ', r.status_code)
        logging.info('Ответ сервера для страницы %s %d', url_page, r.status_code)
        sql_obj.write_event_in_system_log(sql_obj.current_date(), name_program, comment_prog, 1)
        con.close()
        quit()

    # ticker_list = ['21 Century Fox', '3M Company', 'AbbVie', 'Adobe', 'AIG Inc.', 'Airbnb, Inc.', 'Alibaba',
    #                'Alphabet C', 'Altria Group', 'Amazon', 'American Air.', 'American Ex.', 'Amgen', 'Apple',
    #                'AT&T', 'Baidu', 'Bank of America', 'Berkshire B', 'Best Buy', 'Biogen', 'BlackRock', 'Boeing',
    #                'Bristol-M', 'Broadcom', 'Caterpillar', 'Chevron', 'Cisco', 'Cleveland-Cliffs Inc', 'CLF',
    #                'CME Group', 'Coca-Cola', 'Comcast', 'ConocoPh.', 'CVS Health Corp.', 'D.R. Horton, Inc.',
    #                'Datadog, Inc. Class A', 'Delta Air Lines', 'Dow Inc.', 'DraftKings Inc. Class A', 'eBay',
    #                'Exxon Mobil', 'Facebook', 'FedEx Со', 'Ferrari', 'FirstSolar', 'Ford Motor', 'General Electric',
    #                'General Mot.', 'Gilead Sciences', 'Goldman Sachs', 'Harley-David.', 'Hasbro', 'Home Depot',
    #                'Honeywell', 'HP Inc.', 'HubSpot Inc.', 'IBM', 'Intel', 'Jacobs Engineering Group Inc.',
    #                'JD.com, Inc.', 'Johnson & Johnson', 'JPMorgan Chase', 'Kinder Morgan', 'Kraft Heinz',
    #                'Lockheed Martin', "Macy's, Inc.", "McDonald's", 'Medtronic PLC', 'Merck & Co', 'MetLife',
    #                'Micron Tech', 'Microsoft', 'Morgan St', 'Netflix', 'Newmont', 'Nike', 'NRG En', 'NVIDIA',
    #                'PayPal', 'Pfizer', 'Philip Morris', 'Phillips 66', 'Procter & Gamble', 'Qualcomm',
    #                'salesforce.com', 'Schlumberger', 'Square ', 'Starbucks', 'TCS Group (СПБ Биржа)', 'Tesla',
    #                'Twilio Inc. Class A', 'Twitter', 'U.S. Bancorp', 'Union Pacific', 'United Parcel', 'Valero',
    #                'Verizon', 'Virgin Galactic Holdings, Inc.', 'VISA', 'Walgreens Boots', 'WalMart',
    #                'Walt Disney', 'Wells Fargo', 'Zoom Video Communications Inc. C']
    #
    # recommendation_list = ['Покупать', 'Держать', 'Покупать', 'Покупать', 'Покупать', 'Покупать', 'Покупать',
    #                        'Покупать',
    #                        'Покупать', 'Покупать', 'Держать', 'Покупать', 'Покупать', 'Покупать', 'Покупать',
    #                        'Покупать',
    #                        'Покупать', 'Покупать', 'Покупать', 'Держать', 'Покупать', 'Держать', 'Покупать', 'Покупать',
    #                        'Покупать', 'Покупать', 'Покупать', 'Держать', 'Держать', 'Держать', 'Держать', 'Покупать',
    #                        'Покупать', 'Покупать', 'Покупать', 'Покупать', 'Покупать', 'Держать', 'Покупать', 'Держать',
    #                        'Покупать', 'Покупать', 'Покупать', 'Покупать', 'Покупать', 'Держать', 'Покупать',
    #                        'Покупать',
    #                        'Покупать', 'Покупать', 'Покупать', 'Покупать', 'Покупать', 'Покупать', 'Покупать',
    #                        'Покупать',
    #                        'Держать', 'Продавать', 'Покупать', 'Покупать', 'Покупать', 'Покупать', 'Держать', 'Держать',
    #                        'Покупать', 'Продавать', 'Покупать', 'Покупать', 'Покупать', 'Покупать', 'Покупать',
    #                        'Покупать',
    #                        'Покупать', 'Покупать', 'Покупать', 'Покупать', 'Покупать', 'Покупать', 'Покупать',
    #                        'Держать',
    #                        'Покупать', 'Покупать', 'Покупать', 'Покупать', 'Покупать', 'Покупать', 'Покупать',
    #                        'Покупать',
    #                        'Покупать', 'Покупать', 'Покупать', 'Держать', 'Держать', 'Покупать', 'Покупать', 'Покупать',
    #                        'Держать', 'Продавать', 'Покупать', 'Держать', 'Покупать', 'Покупать', 'Покупать',
    #                        'Покупать']
    #
    # target_list = ['41.43', '207.25', '124.50', '629.09', '58.67', '167.86', '288.41', '3053.75', '52.83',
    #                '4198.68', '18.04', '187.62', '262.75', '169.44', '31.00', '245.63', '44.73', '95.13', '129.60',
    #                '408.58', '1030.50', '262.25', '80.55', '544.73', '230.82', '122.50', '59.83', '30.00', '30.0000',
    #                '216.60', '61.44', '69.00', '73.67', '98.78', '116.50', '155.78', '55.08', '68.40', '66.32',
    #                '71.89', '68.18', '410.25', '348.27', '235.40', '99.70', '15.30', '114.83', '75.91', '77.80',
    #                '425.25', '54.00', '116.90', '356.50', '246.22', '31.57', '712.36', '148.25', '57.79', '160.33',
    #                '92.46', '185.80', '167.10', '18.55', '41.22', '450.86', '23.50', '265.13', '146.71', '96.09',
    #                '69.75', '120.57', '336.93', '104.36', '579.05', '76.33', '183.15', '47.67', '233.53', '339.05',
    #                '45.10', '112.33', '88.88', '159.00', '178.40', '304.24', '37.96', '304.30', '130.57', '84.31',
    #                '674.45', '469.42', '76.93', '64.7500', '250.86', '218.21', '84.44', '61.10', '27.47', '284.47',
    #                '52.29', '170.75', '214.07', '49.88', '386.64']

    target_list_ = []
    size_frame = len(ticker_list)
    date_current = datetime.now()
    for i in range(size_frame):
        target = float(target_list[i])
        if target < 1:
            target = round(target, 5)
        else:
            target = round(target, 2)
        target_list_.append(target)

    # перевод рекоммендаций в стандарт yahoo
    recommendation_list_ = []
    size_frame = len(recommendation_list)
    for i in range(size_frame):
        if recommendation_list[i] == 'Покупать':
            recommendation_list_.append(1.0)
        elif recommendation_list[i] == 'Держать':
            recommendation_list_.append(3.0)
        elif recommendation_list[i] == 'Продавать':
            recommendation_list_.append(5.0)
        else:
            print('Не известная рекоммендация ', recommendation_list[i], ' для тикера ',
                  ticker_list[i])
            logging.warning('Не известная рекоммендация %s для тикера %s',
                  recommendation_list[i], ticker_list[i])
            recommendation_list_.append(0.0)
    # print(recommendation_list_)

    # перевод названий с сайта в тикеры
    usa_dict = {'3M Company': 'MMM', 'AbbVie': 'ABBV', 'Adobe': 'ADBE', 'AIG': 'AIG', 'Airbnb': 'ABNB',
                'Alibaba': 'BABA', 'Alphabet Class C': 'GOOG', 'Altria Group': 'MO', 'Amazon': 'AMZN',
                'American Airlines Group': 'AAL', 'American Express': 'AXP', 'Amgen': 'AMGN', 'Apple': 'AAPL', 'AT&T': 'T',
                'Baidu': 'BIDU', 'Bank of America': 'BAC', 'Berkshire Hathaway Class B': 'BRK-B', 'Best Buy': 'BBY', 'Biogen': 'BIIB',
                'BlackRock': 'BLK', 'Boeing': 'BA', 'Bristol-Myers Squibb': 'BMY', 'Broadcom': 'AVGO', 'Caterpillar': 'CAT',
                'Chevron': 'CVX', 'Cisco': 'CSCO', 'Cleveland-Cliffs': 'CLF', 'CLF': 'CLF', 'CME Group': 'CME',
                'Coca-Cola': 'KO', 'Comcast': 'CMCSA', 'ConocoPhillips': 'COP', 'CVS Health': 'CVS',
                'D.R. Horton': 'DHI', 'Datadog Class A': 'DDOG', 'Delta Air Lines': 'DAL',
                'Dow': 'DOW', 'DraftKings Class A': 'DKNG', 'eBay': 'EBAY', 'Exxon Mobil': 'XOM',
                'Facebook': 'FB', 'FedEx': 'FDX', 'Ferrari': 'RACE', 'FirstSolar': 'FSLR', 'Ford Motor': 'F',
                'General Electric': 'GE', 'General Motors': 'GM', 'Gilead Sciences': 'GILD', 'Goldman Sachs': 'GS',
                'Harley-Davidson': 'HOG', 'Hasbro': 'HAS', 'Home Depot': 'HD', 'Honeywell': 'HON', 'HP Inc.': 'HPQ',
                'HubSpot': 'HUBS', 'IBM': 'IBM', 'Intel': 'INTC', 'Jacobs Engineering Group': 'J',
                'JD.com': 'JD', 'Johnson & Johnson': 'JNJ', 'JPMorgan Chase': 'JPM', 'Kinder Morgan': 'KMI',
                'Kraft Heinz': 'KHC', 'Lockheed Martin': 'LMT', "Macy's": 'M', "McDonald's": 'MCD',
                'Medtronic': 'MDT', 'Merck & Co': 'MRK', 'MetLife': 'MET', 'Micron Technology': 'MU', 'Microsoft': 'MSFT',
                'Morgan Stanley': 'MS', 'Netflix': 'NFLX', 'Newmont Corporation': 'NEM', 'Nike': 'NKE', 'NRG Energy': 'NRG',
                'NVIDIA': 'NVDA', 'PayPal': 'PYPL', 'Pfizer': 'PFE', 'Philip Morris': 'PM', 'Phillips 66': 'PSX',
                'Procter & Gamble': 'PG', 'Qualcomm': 'QCOM', 'salesforce.com': 'CRM', 'Schlumberger': 'SLB',
                'Square': 'SQ', 'Starbucks': 'SBUX', 'TCS Group Holdings': 'TCSG', 'Tesla': 'TSLA',
                'Twilio Class A': 'TWLO', 'Twitter': 'TWTR', 'U.S. Bancorp': 'USB', 'Union Pacific': 'UNP',
                'United Parcel': 'UPS', 'Valero': 'VLO', 'Verizon': 'VZ', 'Virgin Galactic Holdings': 'SPCE',
                'VISA': 'V', 'Walgreens Boots': 'WBA', 'WalMart': 'WMT', 'Walt Disney': 'DIS', 'Wells Fargo': 'WFC',
                'Zoom Video Communications': 'ZM'}

    ticker_list_ = []
    size_frame = len(ticker_list)
    for i in range(size_frame):
        if ticker_list[i] in usa_dict:
            ticker_list_.append(usa_dict[ticker_list[i]])
        else:
            print('Нет тикера ', ticker_list[i], ' в словаре usa_dict')
            logging.warning('Нет тикера %s в словаре usa_dict', ticker_list[i])
            ticker_list_.append('abcd')
    # print(ticker_list_)

    # создаем итоговый словарь.
    # 'Cleveland-Cliffs Inc': 'CLF', 'CLF': 'CLF', - задвоено, удаляем
    result_list = {}
    size_frame = len(ticker_list_)
    except_clf = False
    for i in range(size_frame):
        if ticker_list_[i] == 'CLF' and except_clf is False:
            result_list[ticker_list_[i]] = (recommendation_list_[i], target_list_[i])
            except_clf = True
        elif ticker_list_[i] == 'CLF' and except_clf is True:
            pass
        else:
            result_list[ticker_list_[i]] = (recommendation_list_[i], target_list_[i])
    # print(result_list)
    # {'QIWI': (3.0, 909.9), 'AKRN': (3.0, 6850.0),

    # проверим существование сводной таблицы Tickers
    # таблица создается вручную и потом заполняется начальными данными по тикерам и биржам
    if sql_obj.check_table_is_exists(cursorObj, name_main_table):
        # print("Таблица ", name_main_table, " существует")
        logging.info('Таблица %s существует', name_main_table)
        # организуем цикл по всем тикерам из сводной таблицы
        q = "SELECT * FROM {table}"
        q_mod = q.format(table=name_main_table)
        cursorObj.execute(q_mod)
        rows = cursorObj.fetchall()
        for row in rows:
            # перебор начинаем с указанного индекса (сраниваем с primary key)
            if row[0] >= index_ticker and row[10] == 'Yes':
                current_ticker = row[1]
                logging.info('Текущий тикер %s', current_ticker)
                # Ticker_ID, Ticker, Ticker_name, ISIN_code, Exchange, Sector,
                # Industry, Date_r_avto, Recom_avto, Lot, Listing, Target

                # если для текущего тикера есть данные, работаем с ними
                if current_ticker in result_list:
                    # загрузка текущих рекоммендаций аналитиков и таргетов
                    # result_list[current_ticker] (1.0, 46.41)
                    # рекоммендации с БКС не грузим для американского рынка

                    target_trend_modul(con, cursorObj, current_ticker, result_list[current_ticker])

                    print("Получили информацию по тикеру ", current_ticker)
                    logging.info('Получили информацию по тикеру %s', current_ticker)
    else:
        print("Таблица ", name_main_table, " не существует")
        logging.warning('Таблица %s не существует', name_main_table)
        sql_obj.write_event_in_system_log(con, cursorObj, sql_obj.current_date(), name_program, comment_prog, 1)
        con.close()
        quit()

    # запускаем программу для обновления цен в _Compare_target и обновления тикера в _Tickers
    compare_target = anm.AnalystModul(path, name_db, name_main_table, index_ticker)
    compare_target.manipulation_targets()
    sql_obj.write_event_in_system_log(con, cursorObj, sql_obj.current_date(), name_program, comment_prog, 0)
    con.close()

