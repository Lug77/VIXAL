import requests
from bs4 import BeautifulSoup
from portfolio_package import sqlite_modul as sqm
import configparser
import os
import logging
from time import sleep
from datetime import datetime
from dateutil.parser import parse
import random

name_program = 'parser_bcs'
comment_prog = 'загрузка истории дивидендов для российского рынка с сайта bcs-express.ru'

path_config_file = 'settings_expert_system.ini'   # файл с конфигурацией

# user-agent берется из свойств браузера через консоль
name_user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36'
headers = {'user-agent': name_user_agent, 'accept': '*/*'}

# база данных
# путь к БД
path = 'C:\DB_RUS'
name_db = 'rus_market.db'
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

    # парсер используется только для базы rus, поэтому сразу выбираем нужный путь из config-файла
    path = config.get("Settings", "path_1")
    name_db = config.get("Settings", "name_db_1")
    name_main_table = config.get("Settings", "name_main_table")
    index_ticker = int(config.get("Settings", "index_ticker"))


def create_url_request(ticker: str):
    # перевод имени тикера в нижний регистр
    ticker_ = ticker.lower()
    return 'https://bcs-express.ru/kotirovki-i-grafiki/' + ticker_


####################################################################################################
logging.basicConfig(filename='parser_bcs.log',
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
            if row[0] >= index_ticker:
                current_ticker = row[1]
                logging.info('Текущий тикер %s', current_ticker)
                # формируем запрос
                r = requests.get(create_url_request(current_ticker), headers=headers, params=None)
                if r.status_code == 200:
                    # получен ответ сервера
                    print('Ответ сервера для тикера ', current_ticker, ' ', r.status_code)
                    logging.info('Ответ сервера для тикера %s %d', current_ticker, r.status_code)
                    soup = BeautifulSoup(r.text, 'html.parser')

                    # получим данные для сводной таблицы
                    info_data = {}
                    items = soup.find_all('div', class_='quote-emitent__data-item')
                    for item in items:
                        q = item.find(class_='quote-emitent__data-title')
                        if q:
                            key = q.get_text()
                            q_ = item.find(class_='quote-emitent__data-value')
                            if q_:
                                value = q_.get_text()
                                info_data[key] = value
                    # {'Акция': 'АФК Система', 'Номинал': '0,09', 'free-float': '33,00',
                    #  'Полное название': 'АФК "Система" ОАО ао',
                    #  'Капитализация': '297,99 млрд.', 'Тип': 'Обыкновенная', 'Количество': '9\xa0650\xa0000\xa0000',
                    #  'ISIN-код': 'RU000A0DQZE3', 'Отрасль': 'Потребительский сектор'}
                    # заполняем не достающие данные в сводной таблице
                    ticker_name = row[2]
                    ticker_isin_code = row[3]
                    ticker_exchange = row[4]
                    ticker_sector = row[5]
                    if ticker_name == '':
                        if 'Акция' in info_data:
                            sql_obj.update_date_in_cell(con, cursorObj, name_main_table,
                                                        'Ticker_name', info_data['Акция'], 'Ticker',
                                                        current_ticker)
                    if ticker_isin_code == '' or ticker_isin_code == '-':
                        if 'ISIN-код' in info_data:
                            sql_obj.update_date_in_cell(con, cursorObj, name_main_table,
                                                        'ISIN_code', info_data['ISIN-код'], 'Ticker',
                                                        current_ticker)
                    if ticker_exchange == '':
                        sql_obj.update_date_in_cell(con, cursorObj, name_main_table,
                                                        'Exchange', 'MICEX', 'Ticker',
                                                        current_ticker)
                    if ticker_sector == '':
                        if 'Отрасль' in info_data:
                            sql_obj.update_date_in_cell(con, cursorObj, name_main_table,
                                                        'Sector', info_data['Отрасль'], 'Ticker',
                                                        current_ticker)

                    # дивиденды
                    # получим название дивиденда
                    list_name_dividends = []
                    items = soup.find_all('div', class_='dividends-table__row _item')
                    for item in items:
                        # последний
                        q = item.find(class_='dividends-table__cell _title _toggle js-div-table-toggle')
                        if q:
                            name_dividends = q.get_text()
                            list_name_dividends.append(name_dividends)
                        # исторический
                        q = item.find(class_='dividends-table__cell _title')
                        if q:
                            name_dividends = q.get_text()
                            list_name_dividends.append(name_dividends)
                    # ['Аэрофлот ао 2018', 'Аэрофлот ао 2017', 'Аэрофлот ао 2016', 'Аэрофлот ао 2013', 'Аэрофлот ао 2012',
                    #  'Аэрофлот ао 2011', 'Аэрофлот ао 2010', 'Аэрофлот ао 2009', 'Аэрофлот ао 2008', 'Аэрофлот ао 2007',
                    #  'Аэрофлот ао 2006', 'Аэрофлот ао 2005', 'Аэрофлот ао 2004', 'Аэрофлот ао 2003', 'Аэрофлот ао 2002',
                    #  'Аэрофлот ао 2001', 'Аэрофлот ао 2000', 'Аэрофлот ао 2015', 'Аэрофлот ао 2014']

                    # получим дату закрытия реестра
                    list_payment_date = []
                    items = soup.find_all('div', class_='dividends-table__row _item')
                    for item in items:
                        # последний
                        q = item.find(class_='dividends-table__cell _last-day')
                        if q:
                            payment_date = q.get_text()
                            list_payment_date.append(payment_date)
                        # исторический
                        # q = item.find(class_='dividends-table__cell _last-day')
                        # if q:
                        #     payment_date = q.get_text()
                        #     list_payment_date.append(payment_date)
                    # ['03.07.2019', '04.07.2018', '12.07.2017', '04.07.2014', '06.05.2013', '10.05.2012', '11.05.2011',
                    #  '04.05.2010', '05.05.2009', '05.05.2008', '07.05.2007', '29.04.2006', '30.04.2005', '19.04.2004',
                    #  '07.03.2003', '05.04.2002', '20.03.2001', '—', '—']

                    # получим размер выплаты
                    list_dividends = []
                    items = soup.find_all('div', class_='dividends-table__row _item')
                    for item in items:
                        # последний
                        q = item.find(class_='dividends-table__subvalue')
                        if q:
                            dividend = q.get_text()
                            list_dividends.append(dividend)
                        # исторический
                        # q = item.find(class_='dividends-table__subvalue')
                        # if q:
                        #     dividend = q.get_text()
                        #     list_dividends.append(dividend)
                    # ['2.6877', '12.8053', '17.4795', '2.4984', '1.1636', '1.8081', '1.0851', '0.3497', '0.1818',
                    #  '1.367', '1.287', '0.8202', '0.7', '0.4369', '0.29', '0.06', '0.03', '0', '0']

                    # проверим существование таблицы для загрузки
                    name_table_dividends = current_ticker + '_Dividends'
                    if sql_obj.check_table_is_exists(cursorObj, name_table_dividends):
                        print("Таблица ", name_table_dividends, " существует")
                        logging.info('Таблица %s существует', name_table_dividends)
                    else:
                        # таблицы не существует. создадим ее, если есть данные
                        if list_name_dividends is None or len(list_name_dividends) == 0:
                            pass
                        else:
                            logging.warning('Таблица %s не обнаружена. Создадим ее', name_table_dividends)
                            q = """ CREATE TABLE {table}
                                        (
                                        Ticker text NOT NULL,
                                        Payment_date date DEFAULT '1970.01.01',
                                        Dividends real DEFAULT 0.0
                                        )
                                        """
                            q_mod = q.format(table=name_table_dividends)
                            # print(q_mod)
                            cursorObj.execute(q_mod)
                            con.commit()

                    # заполняем таблицу
                    # предварительно соберем данные в 3-х мерный массив и отсортируем по дате по возрастанию
                    if list_name_dividends is None or len(list_name_dividends) == 0:
                        pass
                    else:
                        # [[name,date,payment],[name,date,payment],...]
                        final_list = []
                        q_ = []
                        size_frame = len(list_name_dividends)
                        for i in range(size_frame):
                            q_.append(list_name_dividends[i])

                            try:
                                date_payment = parse(list_payment_date[i])
                            except:
                                date_payment = parse('01.01.2000')
                            q_.append(date_payment)

                            # перевод размера выплаты во float с учетом валюты выплаты
                            dividend_ = sql_obj.convert_str_to_float_dividend(list_dividends[i], date_payment)
                            q_.append(dividend_)
                            final_list.append(q_)
                            q_ = []
                        # сортировка по дате (по возрастанию)
                        final_list.sort(key=lambda x: x[1])

                        if sql_obj.check_table_is_exists(cursorObj, name_table_dividends):
                            # запускаем цикл
                            size_frame = len(final_list)
                            for i in range(size_frame):
                                q = '''INSERT INTO {table} (Ticker, Payment_date, Dividends) 
                                                    VALUES(?, ?, ?)'''
                                q_mod = q.format(table=name_table_dividends)

                                # перед загрузкой проверим дату на уникальность для текущей таблицы
                                # преобразование timestamp в datetime
                                d = str(final_list[i][1])
                                d_mod = datetime.strptime(d[0:10], "%Y-%m-%d")
                                # если значение не найдено, записываем его
                                find_value = (d_mod,)
                                q_1 = "SELECT {column_1} FROM {table} WHERE {column_1}=?"
                                q_1_mod = q_1.format(table=name_table_dividends, column_1='Payment_date')
                                cursorObj.execute(q_1_mod, find_value)
                                if cursorObj.fetchone() is None:
                                    # заносим в таблицу данные
                                    cursorObj.execute(q_mod, (final_list[i][0], d_mod, final_list[i][2]))
                                    con.commit()
                        else:
                            print("Таблица ", name_table_dividends, " не существует")
                            logging.warning('Таблица %s не существует', name_table_dividends)
                else:
                    # ошибка
                    print('Ответ сервера для тикера ',  current_ticker, ' ', r.status_code)
                    logging.warning('Ответ сервера для тикера %s %d', current_ticker, r.status_code)

                # включаем случайную задержку от 30 до 100 сек перед новым запросом на сервер
                print("Получили информацию по тикеру ", current_ticker)
                logging.info('Получили информацию по тикеру %s', current_ticker)
                sleep(random.randint(30, 100))
    else:
        print("Таблица ", name_main_table, " не существует")
        logging.warning('Таблица %s не существует', name_main_table)
        sql_obj.write_event_in_system_log(con, cursorObj, sql_obj.current_date(), name_program, comment_prog, 1)
        con.close()
        quit()

    sql_obj.write_event_in_system_log(con, cursorObj, sql_obj.current_date(), name_program, comment_prog, 0)
    con.close()
