# загрузка информации о символе и котировок h1 и d1 в БД через терминал МТ5

import MetaTrader5 as mt5
import pandas as pd
import os
from datetime import datetime, date, time, timedelta
from dateutil.parser import parse
from portfolio_package import sqlite_modul as sqm
import configparser
import logging
import pytz
import argparse

path_config_file = 'settings_expert_system.ini'   # файл с конфигурацией

# аргумент из командной строки для выбора каталога БД
dir_db = 'RUS'

# название БД
path = 'C:\DB_RUS'
name_db = 'rus_market.db'

# имя сводной таблицы
name_main_table = '_Tickers'

# индекс тикера с которого начинать загрузку (=1 - первый тикер в сводной таблице)
index_ticker = 1


# функция для загрузки файла конфигурации
def crud_config(path_, obj_args):
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

    # аргумент из командной строки для выбора каталога БД
    global dir_db
    dir_db = obj_args.dir_db

    # Читаем значения из конфиг. файла и присваиваем их глобальным переменным
    global path
    global name_db
    global name_main_table
    global index_ticker

    # выбираем путь к БД в зависимости от аргумента в командной строке
    if dir_db == 'usa' or dir_db == 'USA':
        path = config.get("Settings", "path")
        name_db = config.get("Settings", "name_db")
        print('Работаем с Базой 1 path ', path, " name_db ", name_db)
        logging.info('Работаем с Базой 1 path %s name_db %s_1', path, name_db)
    elif dir_db == 'rus' or dir_db == 'RUS':
        path = config.get("Settings", "path_1")
        name_db = config.get("Settings", "name_db_1")
        print('Работаем с Базой 2 path ', path, " name_db ", name_db)
        logging.info('Работаем с Базой 2 path %s name_db %s_1', path, name_db)
    else:
        # используются значения по умолчанию
        print('Используются значения по умолчанию path ', path, " name_db ", name_db)
        logging.info('Валюта портфеля не определена. Используются значения по умолчанию path %s name_db %s_1', path,
                     name_db)
        logging.info('Используются значения по умолчанию path %s name_db %s_1', path, name_db)

    name_main_table = config.get("Settings", "name_main_table")
    index_ticker = int(config.get("Settings", "index_ticker"))


####################################################################################################
logging.basicConfig(filename='mt5_agent_quotes.log',
                    format='[%(asctime)s] [%(levelname)s] => %(message)s',
                    filemode='w',
                    level=logging.DEBUG)

parser = argparse.ArgumentParser(description='Path to DB')
# определяем необязательный аргумент как --
parser.add_argument('--dir_db', type=str, help='Directory DB')
args = parser.parse_args()

# загрузка параметров из файла конфигурации
crud_config(path_config_file, args)
print('Load settings from configuration files: ')
print('path: ', path)
print('name_db : ', name_db)
print('name_main_table : ', name_main_table)
print('index_ticker : ', index_ticker)

filename = path + '\\' + name_db
print('Используется БД: ', filename)
logging.info('Используется БД: %s', filename)

sql_obj = sqm.SQLite()

if 1 == 1:
    # старт загрузки котировок в БД
    # проверка наличия БД
    if sql_obj.check_db(filename) == False:
        print("База данных ", filename, " не обнаружена")
        logging.warning('База данных %s не обнаружена', filename)
        quit()
    else:
        print("База данных ", filename, " существует")
        logging.info('База данных %s существует', filename)

    # устанавливаем соединение и создаем обьект курсора
    con = sql_obj.create_connection(filename)
    cursorObj = con.cursor()

    # подключимся к нужному MetaTrader 5
    # путь к файлу metatrader.exe или metatrader64.exe находится в системной переменной в таблице _System_variables
    # для котировок акций path_mt5_moex
    # для котировок индексов path_mt5_forts
    # сначала получаем котировки акций
    path_mt5 = sql_obj.get_system_var_value(cursorObj, 'path_mt5_moex', 'text')
    # print(path_mt5)
    if not mt5.initialize(path_mt5):
        print("Не удалось установить соединение с терминалом МТ5")
        logging.warning('Не удалось установить соединение с терминалом МТ5')
        mt5.shutdown()
        quit()

    # запросим статус и параметры подключения
    info_mt5 = mt5.terminal_info()
    # print(mt5.terminal_info())
    # да, нашел открытый мт5
    # TerminalInfo(community_account=True, community_connection=True, connected=True,
    # dlls_allowed=False,
    # trade_allowed=False,
    # tradeapi_disabled=False,
    # email_enabled=False,
    # ftp_enabled=False,
    # notifications_enabled=False,
    # mqid=False,
    # build=2981,
    # maxbars=100000,
    # codepage=1251,
    # ping_last=7395,
    # community_balance=17.033517,
    # retransmission=0.0,
    # company="АО ''Открытие Брокер''",
    # name='Открытие Брокер',
    # language='Russian',
    # path='C:\\Program Files\\Открытие Брокер',
    # data_path='C:\\Users\\lugov\\AppData\\Roaming\\MetaQuotes\\Terminal\\1B9501BF48F2354A4685940A72752910',
    # commondata_path='C:\\Users\\lugov\\AppData\\Roaming\\MetaQuotes\\Terminal\\Common')
    print('Установлено соединение с терминалом МТ5 из рабочей папки: ')
    print(info_mt5[20])
    logging.info('Установлено соединение с терминалом МТ5 из рабочей папки: %s', info_mt5[20])

    # получим информацию о версии MetaTrader 5
    # print(mt5.version())

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
                # print(row[1])
                logging.info('Текущий тикер %s', current_ticker)

                # проверка символа в терминале
                selected = mt5.symbol_select(current_ticker, True)
                if selected:
                    # получим свойства символа в виде словаря
                    # одна из задач проверка лотности в сводной таблице и установка ее
                    symbol_info_dict = mt5.symbol_info(current_ticker)._asdict()
                    # for prop in symbol_info_dict:
                    #     print("  {}={}".format(prop, symbol_info_dict[prop]))
                        # trade_contract_size = 10 ---------------------> лотность
                        # bank =
                        # description = Сбербанк
                        # isin = RU0009029540
                        # ну и много чего еще
                    # print(symbol_info_dict.keys())
                    lot_ticker = symbol_info_dict['trade_contract_size']
                    # print(current_ticker, ' лотность ', lot_ticker, ' ', type(lot_ticker))
                    if row[9] != lot_ticker:
                        # изменим лотность
                        sql_obj.update_date_in_cell(con, cursorObj, name_main_table,
                                                    'Lot', lot_ticker, 'Ticker',
                                                    current_ticker)

                    # проверим существование таблицы для загрузки часовых котировок
                    name_table_price_hour = current_ticker + '_' + '1h'
                    # если таблицы нет, то это будет 1-ая загрузка и ее можно выполнить путем
                    # множественной загрузки строк
                    # если таблица есть, то перед загрузкой данных нужно проверять есть ли такие данные в таблице
                    # флаг первичной загрузки данных
                    first_loading_data = False
                    if sql_obj.check_table_is_exists(cursorObj, name_table_price_hour):
                        print("Таблица ", name_table_price_hour, " существует")
                        logging.info('Таблица %s существует', name_table_price_hour)
                    else:
                        print("Таблица ", name_table_price_hour, " не обнаружена")
                        logging.warning('Таблица %s не обнаружена. Создадим ее', name_table_price_hour)
                        # создадим таблицу для загрузки котировок
                        sql_obj.create_table_market_data(con, cursorObj, name_table_price_hour)
                        first_loading_data = True

                    # если таблицы еще нет (1-ая загрузка, получим данные за последние 2 года 2*252*14=7056)
                    # если таблица уже есть, получим недостающие данные
                    bars = 0
                    # дата и время с которых нужно запросить котировки
                    # установим таймзону в UTC
                    timezone = pytz.timezone("Etc/UTC")
                    date_from = datetime.now()
                    # дата и время последнего закрытого бара для таймфрейма Н1
                    delta = timedelta(hours=1)
                    date_to = datetime(date_from.year, date_from.month, date_from.day,
                                       date_from.hour, 0, 0, tzinfo=timezone) - delta
                    if first_loading_data is False:
                        # получим последнюю строку из таблицы
                        last_row = sql_obj.get_last_row(cursorObj, name_table_price_hour, "Date")
                        if last_row is not None:
                            # последняя дата и время в таблице
                            if last_row[0] is not None:
                                date_from = parse(last_row[0])
                                date_from = datetime(date_from.year, date_from.month, date_from.day,
                                                     date_from.hour, 0, 0, tzinfo=timezone) + delta
                        else:
                            bars = 5
                    else:
                        bars = 2*252*14

                    if bars != 0:
                        ticker_rates = mt5.copy_rates_from_pos(current_ticker, mt5.TIMEFRAME_H1, 0, bars)
                    else:
                        # print('type(date_from) ', type(date_from), ' date_from ', date_from)
                        # print('type(date_to) ', type(date_to), ' date_to ', date_to)
                        ticker_rates = mt5.copy_rates_range(current_ticker, mt5.TIMEFRAME_H1, date_from, date_to)

                    # создадим из полученных данных DataFrame
                    data = pd.DataFrame(ticker_rates)
                    # print(data.columns)
                    # ['time', 'open', 'high', 'low', 'close', 'tick_volume', 'spread', 'real_volume']
                    # print(data)
                    if data is None or len(data) == 0:
                        print("Не удалось прочитать часовые данные по символу ", current_ticker, ' или их еще нет')
                        logging.warning("Не удалось прочитать часовые данные по символу %s или их еще нет",
                                        current_ticker)
                        continue
                    else:
                        # начало данных [0]
                        # если делать множественную загрузку в рабочее время
                        # последний бар получается не полный, поэтому просто обрезаем последнюю строку
                        if bars != 0:
                            size_frame = len(data)
                            data = data.drop(index=[size_frame-1])
                        # добавляем новые столбцы
                        data['datetime'] = pd.to_datetime(data['time'], unit='s')
                        data.index = data['datetime']
                        data['date'] = data['datetime'].dt.date
                        data['time'] = data['datetime'].dt.time
                        # print(data)

                        if first_loading_data:
                            # данных в таблице еще нет - множественная загрузка
                            logging.info('Данных в таблице еще нет - множественная загрузка строк')
                            # формируем список для загрузки в БД
                            data_load = []
                            size_frame = len(data)
                            for i in range(size_frame):
                                row = (current_ticker,
                                       '1h',
                                       datetime.combine(data['date'][i], data['time'][i]),
                                       data['open'][i],
                                       data['high'][i],
                                       data['low'][i],
                                       data['close'][i],
                                       data['close'][i],
                                       float(data['real_volume'][i]))
                                data_load.append(row)
                            # print(data_load)
                            # загружаем в БД
                            sql_obj.insert_many_rows(con, cursorObj, name_table_price_hour, data_load)
                        else:
                            # данные в таблице есть - построчная загрузка с проверкой на уникальность
                            logging.info('Данные в таблице есть - построчная загрузка с проверкой на уникальность')
                            size_frame = len(data)
                            for i in range(size_frame):
                                data_load = (current_ticker,
                                             '1h',
                                             datetime.combine(data['date'][i], data['time'][i]),
                                             data['open'][i],
                                             data['high'][i],
                                             data['low'][i],
                                             data['close'][i],
                                             data['close'][i],
                                             float(data['real_volume'][i]))
                                # сделаем запрос к БД по дате и времени
                                select_date = sql_obj.select_date_time(cursorObj, name_table_price_hour,
                                                                       data['date'][i],
                                                                       data['time'][i])
                                if select_date is None:
                                    # котировок с указанной датой нет. загружаем строку в БД
                                    # print(data_load)
                                    sql_obj.insert_one_rows(con, cursorObj, name_table_price_hour, data_load)
                                else:
                                    # котировки с указанной датой есть
                                    pass

                    # проверим существование таблицы для загрузки дневных котировок
                    name_table_price_day = current_ticker + '_' + '1d'
                    # если таблицы нет, то это будет 1-ая загрузка и ее можно выполнить путем
                    # множественной загрузки строк
                    # если таблица есть, то перед загрузкой данных нужно проверять есть ли такие данные в таблице
                    # флаг первичной загрузки данных
                    first_loading_data = False
                    if sql_obj.check_table_is_exists(cursorObj, name_table_price_day):
                        print("Таблица ", name_table_price_day, " существует")
                        logging.info('Таблица %s существует', name_table_price_day)
                    else:
                        print("Таблица ", name_table_price_day, " не обнаружена")
                        logging.warning('Таблица %s не обнаружена. Создадим ее', name_table_price_day)
                        # создадим таблицу для загрузки котировок
                        sql_obj.create_table_market_data(con, cursorObj, name_table_price_day)
                        first_loading_data = True

                    # если таблицы еще нет (1-ая загрузка, получим данные за последние 2 года 2*252=504)
                    # если таблица уже есть, получим недостающие данные
                    bars = 0
                    # дата и время с которых нужно запросить котировки
                    timezone = pytz.timezone("Etc/UTC")
                    date_from = datetime.now()
                    # дата и время последнего закрытого бара для таймфрейма D1
                    delta = timedelta(hours=24)
                    date_to = datetime(date_from.year, date_from.month, date_from.day,
                                       0, 0, 0, tzinfo=timezone) - delta
                    if first_loading_data is False:
                        # получим последнюю строку из таблицы
                        last_row = sql_obj.get_last_row(cursorObj, name_table_price_day, "Date")
                        if last_row is not None:
                            # последняя дата в таблице
                            if last_row[0] is not None:
                                date_from = parse(last_row[0])
                                date_from = datetime(date_from.year, date_from.month, date_from.day,
                                                     0, 0, 0, tzinfo=timezone) + delta
                        else:
                            bars = 5
                    else:
                        bars = 2 * 252

                    if bars != 0:
                        ticker_rates = mt5.copy_rates_from_pos(current_ticker, mt5.TIMEFRAME_D1, 0, bars)
                    else:
                        # print('type(date_from) ', type(date_from), ' date_from ', date_from)
                        # print('type(date_to) ', type(date_to), ' date_to ', date_to)
                        ticker_rates = mt5.copy_rates_range(current_ticker, mt5.TIMEFRAME_D1, date_from, date_to)
                    # создадим из полученных данных DataFrame
                    data = pd.DataFrame(ticker_rates)
                    # print(data.columns)
                    # ['time', 'open', 'high', 'low', 'close', 'tick_volume', 'spread', 'real_volume']
                    # print(data)
                    if data is None or len(data) == 0:
                        print("Не удалось прочитать дневные данные по символу ", current_ticker, ' или их еще нет')
                        logging.warning("Не удалось прочитать дневные данные по символу %s или их еще нет",
                                        current_ticker)
                    else:
                        # начало данных [0]
                        # при множественной загрузке последний бар не полный поэтому просто обрезаем последнюю строку
                        if bars != 0:
                            size_frame = len(data)
                            data = data.drop(index=[size_frame-1])
                        # добавляем новые столбцы
                        data['datetime'] = pd.to_datetime(data['time'], unit='s')
                        data.index = data['datetime']
                        data['date'] = data['datetime'].dt.date
                        data['time'] = data['datetime'].dt.time
                        # print(data)

                        if first_loading_data:
                            # данных в таблице еще нет - множественная загрузка
                            logging.info('Данных в таблице еще нет - множественная загрузка строк')
                            # формируем список для загрузки в БД
                            data_load = []
                            size_frame = len(data)
                            for i in range(size_frame):
                                row = (current_ticker,
                                       '1d',
                                       datetime.combine(data['date'][i], data['time'][i]),
                                       data['open'][i],
                                       data['high'][i],
                                       data['low'][i],
                                       data['close'][i],
                                       data['close'][i],
                                       float(data['real_volume'][i]))
                                data_load.append(row)
                            # print(data_load)
                            # загружаем в БД
                            sql_obj.insert_many_rows(con, cursorObj, name_table_price_day, data_load)
                        else:
                            # данные в таблице есть - построчная загрузка с проверкой на уникальность
                            logging.info('Данные в таблице есть - построчная загрузка с проверкой на уникальность')
                            size_frame = len(data)
                            for i in range(size_frame):
                                data_load = (current_ticker,
                                             '1d',
                                             datetime.combine(data['date'][i], data['time'][i]),
                                             data['open'][i],
                                             data['high'][i],
                                             data['low'][i],
                                             data['close'][i],
                                             data['close'][i],
                                             float(data['real_volume'][i]))
                                # сделаем запрос к БД по дате и времени
                                select_date = sql_obj.select_date_time(cursorObj, name_table_price_day,
                                                                       data['date'][i],
                                                                       data['time'][i])
                                if select_date is None:
                                    # котировок с указанной датой нет. загружаем строку в БД
                                    # print(data_load)
                                    sql_obj.insert_one_rows(con, cursorObj, name_table_price_day, data_load)
                                else:
                                    # котировки с указанной датой есть
                                    pass

                    # проверим существование таблицы для загрузки недельных котировок
                    name_table_price_week = current_ticker + '_' + '1wk'
                    # если таблицы нет, то это будет 1-ая загрузка и ее можно выполнить путем
                    # множественной загрузки строк
                    # если таблица есть, то перед загрузкой данных нужно проверять есть ли такие данные в таблице
                    # флаг первичной загрузки данных
                    first_loading_data = False
                    if sql_obj.check_table_is_exists(cursorObj, name_table_price_week):
                        print("Таблица ", name_table_price_week, " существует")
                        logging.info('Таблица %s существует', name_table_price_week)
                    else:
                        print("Таблица ", name_table_price_week, " не обнаружена")
                        first_loading_data = True

                    # если таблицы еще нет (1-ая загрузка, получим данные c 01.01.2008 - начинается история СБЕР в
                    # терминале)
                    # если таблица уже есть, получим недостающие данные
                    bars = 0
                    # дата и время с которых нужно запросить котировки
                    timezone = pytz.timezone("Etc/UTC")
                    # дата начала интервала
                    date_from = datetime(2008, 1, 1, 0, 0, 0)
                    current_date = datetime.now()
                    # дата конца интервала - дата загрузки
                    date_to = datetime(current_date.year, current_date.month, current_date.day, 0, 0, 0)

                    if first_loading_data is False:
                        # получим последнюю строку из таблицы
                        last_row = sql_obj.get_last_row(cursorObj, name_table_price_week, "Date")
                        if last_row is not None:
                            # последняя дата в таблице
                            if last_row[0] is not None:
                                date_from = parse(last_row[0])
                                delta = timedelta(hours=168)
                                date_from = datetime(date_from.year, date_from.month, date_from.day,
                                                     0, 0, 0, tzinfo=timezone) + delta
                        else:
                            bars = 3

                    if bars != 0:
                        ticker_rates = mt5.copy_rates_from_pos(current_ticker, mt5.TIMEFRAME_W1, 0, bars)
                    else:
                        # print('type(date_from) ', type(date_from), ' date_from ', date_from)
                        # print('type(date_to) ', type(date_to), ' date_to ', date_to)
                        ticker_rates = mt5.copy_rates_range(current_ticker, mt5.TIMEFRAME_W1, date_from, date_to)
                    # создадим из полученных данных DataFrame
                    data = pd.DataFrame(ticker_rates)
                    # print(data.columns)
                    # ['time', 'open', 'high', 'low', 'close', 'tick_volume', 'spread', 'real_volume']

                    if data is None or len(data) == 0:
                        print("Не удалось прочитать недельные данные по символу ", current_ticker, ' или их еще нет')
                        logging.warning("Не удалось прочитать недельные данные по символу %s или их еще нет",
                                        current_ticker)
                    else:
                        # начало данных [0]
                        # добавляем новые столбцы
                        data['datetime'] = pd.to_datetime(data['time'], unit='s')
                        data.index = data['datetime']
                        data['date'] = data['datetime'].dt.date
                        data['time'] = data['datetime'].dt.time
                        # print(data)

                        if first_loading_data:
                            # данных в таблице еще нет - множественная загрузка
                            logging.info('Данных в таблице еще нет - множественная загрузка строк')
                            # формируем список для загрузки в БД
                            data_load = []
                            size_frame = len(data)
                            for i in range(size_frame):
                                # загружать нужно только полные недели
                                # день котировки должен быть воскресенье, полная неделя образуется в субботу
                                # поэтому между датой загрузки (текущая дата) и датой котировки должно быть не
                                # менее 6 дней
                                price_date = data['date'][i]
                                price_day = price_date.day
                                price_day_week = price_date.weekday()
                                delta = date.today() - price_date
                                if price_day_week == 6 and delta.total_seconds() >= 6 * 24 * 60 * 60:
                                    row = (current_ticker,
                                           '1wk',
                                           datetime.combine(data['date'][i], data['time'][i]),
                                           data['open'][i],
                                           data['high'][i],
                                           data['low'][i],
                                           data['close'][i],
                                           data['close'][i],
                                           float(data['real_volume'][i]))
                                    data_load.append(row)
                            # print(data_load)
                            # загружаем в БД
                            if len(data_load) > 0:
                                logging.warning('Таблица %s не обнаружена. Создадим ее', name_table_price_week)
                                # создадим таблицу для загрузки котировок
                                sql_obj.create_table_market_data(con, cursorObj, name_table_price_week)
                                sql_obj.insert_many_rows(con, cursorObj, name_table_price_week, data_load)
                        else:
                            # данные в таблице есть - построчная загрузка с проверкой на уникальность
                            logging.info('Данные в таблице есть - построчная загрузка с проверкой на уникальность')
                            size_frame = len(data)
                            for i in range(size_frame):
                                price_date = data['date'][i]
                                price_day = price_date.day
                                price_day_week = price_date.weekday()
                                delta = date.today() - price_date
                                if price_day_week == 6 and delta.total_seconds() >= 6 * 24 * 60 * 60:
                                    data_load = (current_ticker,
                                                 '1wk',
                                                 datetime.combine(data['date'][i], data['time'][i]),
                                                 data['open'][i],
                                                 data['high'][i],
                                                 data['low'][i],
                                                 data['close'][i],
                                                 data['close'][i],
                                                 float(data['real_volume'][i]))
                                    # сделаем запрос к БД по дате и времени
                                    select_date = sql_obj.select_date_time(cursorObj, name_table_price_week,
                                                                           data['date'][i],
                                                                           data['time'][i])
                                    if select_date is None:
                                        # котировок с указанной датой нет. загружаем строку в БД
                                        # print(data_load)
                                        sql_obj.insert_one_rows(con, cursorObj, name_table_price_week, data_load)
                                    else:
                                        # котировки с указанной датой есть
                                        pass
                else:
                    print('Символа ', current_ticker, ' нет в терминале МТ5')
                    logging.warning('Символа %s нет в терминале МТ5', current_ticker)
                    continue
    # отключамся от терминала
    mt5.shutdown()

    # подключимся к терминалу с индексами
    path_mt5 = sql_obj.get_system_var_value(cursorObj, 'path_mt5_forts', 'text')
    if not mt5.initialize(path_mt5):
        print("Не удалось установить соединение с терминалом МТ5")
        logging.warning('Не удалось установить соединение с терминалом МТ5')
        mt5.shutdown()
        quit()
    # запросим статус и параметры подключения
    info_mt5 = mt5.terminal_info()
    print('Установлено соединение с терминалом МТ5 из рабочей папки: ')
    print(info_mt5[20])
    logging.info('Установлено соединение с терминалом МТ5 из рабочей папки: %s', info_mt5[20])

    # проверим существование сводной таблицы _Market (индексы и прочее)
    # таблица создается вручную и потом заполняется начальными данными по тикерам и биржам
    name_table = '_Market'
    index_market = 1
    if sql_obj.check_table_is_exists(cursorObj, name_table):
        # print("Таблица ", name_table, " существует")
        logging.info('Таблица %s существует', name_table)
        # организуем цикл по всем тикерам из сводной таблицы
        q = "SELECT * FROM {table}"
        q_mod = q.format(table=name_table)
        cursorObj.execute(q_mod)
        rows = cursorObj.fetchall()
        for row in rows:
            # перебор начинаем с указанного индекса (сраниваем с primary key)
            if row[0] >= index_market and row[5] == 'Yes':
                current_ticker = row[1]
                # в качестве тикера для создания таблиц используем колонку 'Ticker_DB'
                ticker_name_table = row[6]
                # print(row[1])
                logging.info('Текущий тикер %s', current_ticker)

                # проверка символа в терминале
                selected = mt5.symbol_select(current_ticker, True)
                if selected:
                    # получим свойства символа в виде словаря
                    symbol_info_dict = mt5.symbol_info(current_ticker)._asdict()

                    # проверим существование таблицы для загрузки часовых котировок
                    name_table_price_hour = ticker_name_table + '_' + '1h'
                    # если таблицы нет, то это будет 1-ая загрузка и ее можно выполнить путем
                    # множественной загрузки строк
                    # если таблица есть, то перед загрузкой данных нужно проверять есть ли такие данные в таблице
                    # флаг первичной загрузки данных
                    first_loading_data = False
                    if sql_obj.check_table_is_exists(cursorObj, name_table_price_hour):
                        print("Таблица ", name_table_price_hour, " существует")
                        logging.info('Таблица %s существует', name_table_price_hour)
                    else:
                        print("Таблица ", name_table_price_hour, " не обнаружена")
                        logging.warning('Таблица %s не обнаружена. Создадим ее', name_table_price_hour)
                        # создадим таблицу для загрузки котировок
                        sql_obj.create_table_market_data(con, cursorObj, name_table_price_hour)
                        first_loading_data = True

                    # если таблицы еще нет (1-ая загрузка, получим данные за последние 2 года 2*252*14=7056)
                    # если таблица уже есть, получим недостающие данные
                    bars = 0
                    # дата и время с которых нужно запросить котировки
                    # установим таймзону в UTC
                    timezone = pytz.timezone("Etc/UTC")
                    date_from = datetime.now()
                    # дата и время последнего закрытого бара для таймфрейма Н1
                    delta = timedelta(hours=1)
                    date_to = datetime(date_from.year, date_from.month, date_from.day,
                                       date_from.hour, 0, 0, tzinfo=timezone) - delta
                    if first_loading_data is False:
                        # получим последнюю строку из таблицы
                        last_row = sql_obj.get_last_row(cursorObj, name_table_price_hour, "Date")
                        if last_row is not None:
                            # последняя дата и время в таблице
                            if last_row[0] is not None:
                                date_from = parse(last_row[0])
                                date_from = datetime(date_from.year, date_from.month, date_from.day,
                                                     date_from.hour, 0, 0, tzinfo=timezone) + delta
                        else:
                            bars = 5
                    else:
                        bars = 2*252*14

                    if bars != 0:
                        ticker_rates = mt5.copy_rates_from_pos(current_ticker, mt5.TIMEFRAME_H1, 0, bars)
                    else:
                        # print('type(date_from) ', type(date_from), ' date_from ', date_from)
                        # print('type(date_to) ', type(date_to), ' date_to ', date_to)
                        ticker_rates = mt5.copy_rates_range(current_ticker, mt5.TIMEFRAME_H1, date_from, date_to)

                    # создадим из полученных данных DataFrame
                    data = pd.DataFrame(ticker_rates)
                    # print(data.columns)
                    # ['time', 'open', 'high', 'low', 'close', 'tick_volume', 'spread', 'real_volume']
                    # print(data)
                    if data is None or len(data) == 0:
                        print("Не удалось прочитать часовые данные по символу ", ticker_name_table, ' или их еще нет')
                        logging.warning("Не удалось прочитать часовые данные по символу %s или их еще нет",
                                        ticker_name_table)
                        continue
                    else:
                        # начало данных [0]
                        # если делать множественную загрузку в рабочее время
                        # последний бар получается не полный, поэтому просто обрезаем последнюю строку
                        if bars != 0:
                            size_frame = len(data)
                            data = data.drop(index=[size_frame-1])
                        # добавляем новые столбцы
                        data['datetime'] = pd.to_datetime(data['time'], unit='s')
                        data.index = data['datetime']
                        data['date'] = data['datetime'].dt.date
                        data['time'] = data['datetime'].dt.time
                        # print(data)

                        if first_loading_data:
                            # данных в таблице еще нет - множественная загрузка
                            logging.info('Данных в таблице еще нет - множественная загрузка строк')
                            # формируем список для загрузки в БД
                            data_load = []
                            size_frame = len(data)
                            for i in range(size_frame):
                                row = (current_ticker,
                                       '1h',
                                       datetime.combine(data['date'][i], data['time'][i]),
                                       data['open'][i],
                                       data['high'][i],
                                       data['low'][i],
                                       data['close'][i],
                                       data['close'][i],
                                       float(data['real_volume'][i]))
                                data_load.append(row)
                            # print(data_load)
                            # загружаем в БД
                            sql_obj.insert_many_rows(con, cursorObj, name_table_price_hour, data_load)
                        else:
                            # данные в таблице есть - построчная загрузка с проверкой на уникальность
                            logging.info('Данные в таблице есть - построчная загрузка с проверкой на уникальность')
                            size_frame = len(data)
                            for i in range(size_frame):
                                data_load = (current_ticker,
                                             '1h',
                                             datetime.combine(data['date'][i], data['time'][i]),
                                             data['open'][i],
                                             data['high'][i],
                                             data['low'][i],
                                             data['close'][i],
                                             data['close'][i],
                                             float(data['real_volume'][i]))
                                # сделаем запрос к БД по дате и времени
                                select_date = sql_obj.select_date_time(cursorObj, name_table_price_hour,
                                                                       data['date'][i],
                                                                       data['time'][i])
                                if select_date is None:
                                    # котировок с указанной датой нет. загружаем строку в БД
                                    # print(data_load)
                                    sql_obj.insert_one_rows(con, cursorObj, name_table_price_hour, data_load)
                                else:
                                    # котировки с указанной датой есть
                                    pass

                    # проверим существование таблицы для загрузки дневных котировок
                    name_table_price_day = ticker_name_table + '_' + '1d'
                    # если таблицы нет, то это будет 1-ая загрузка и ее можно выполнить путем
                    # множественной загрузки строк
                    # если таблица есть, то перед загрузкой данных нужно проверять есть ли такие данные в таблице
                    # флаг первичной загрузки данных
                    first_loading_data = False
                    if sql_obj.check_table_is_exists(cursorObj, name_table_price_day):
                        print("Таблица ", name_table_price_day, " существует")
                        logging.info('Таблица %s существует', name_table_price_day)
                    else:
                        print("Таблица ", name_table_price_day, " не обнаружена")
                        logging.warning('Таблица %s не обнаружена. Создадим ее', name_table_price_day)
                        # создадим таблицу для загрузки котировок
                        sql_obj.create_table_market_data(con, cursorObj, name_table_price_day)
                        first_loading_data = True

                    # если таблицы еще нет (1-ая загрузка, получим данные за последние 2 года 2*252=504)
                    # если таблица уже есть, получим недостающие данные
                    bars = 0
                    # дата и время с которых нужно запросить котировки
                    timezone = pytz.timezone("Etc/UTC")
                    date_from = datetime.now()
                    # дата и время последнего закрытого бара для таймфрейма D1
                    delta = timedelta(hours=24)
                    date_to = datetime(date_from.year, date_from.month, date_from.day,
                                       0, 0, 0, tzinfo=timezone) - delta
                    if first_loading_data is False:
                        # получим последнюю строку из таблицы
                        last_row = sql_obj.get_last_row(cursorObj, name_table_price_day, "Date")
                        if last_row is not None:
                            # последняя дата в таблице
                            if last_row[0] is not None:
                                date_from = parse(last_row[0])
                                date_from = datetime(date_from.year, date_from.month, date_from.day,
                                                     0, 0, 0, tzinfo=timezone) + delta
                        else:
                            bars = 5
                    else:
                        bars = 2 * 252

                    if bars != 0:
                        ticker_rates = mt5.copy_rates_from_pos(current_ticker, mt5.TIMEFRAME_D1, 0, bars)
                    else:
                        # print('type(date_from) ', type(date_from), ' date_from ', date_from)
                        # print('type(date_to) ', type(date_to), ' date_to ', date_to)
                        ticker_rates = mt5.copy_rates_range(current_ticker, mt5.TIMEFRAME_D1, date_from, date_to)
                    # создадим из полученных данных DataFrame
                    data = pd.DataFrame(ticker_rates)
                    # print(data.columns)
                    # ['time', 'open', 'high', 'low', 'close', 'tick_volume', 'spread', 'real_volume']
                    # print(data)
                    if data is None or len(data) == 0:
                        print("Не удалось прочитать дневные данные по символу ", ticker_name_table, ' или их еще нет')
                        logging.warning("Не удалось прочитать дневные данные по символу %s или их еще нет",
                                        ticker_name_table)
                    else:
                        # начало данных [0]
                        # при множественной загрузке последний бар не полный поэтому просто обрезаем последнюю строку
                        if bars != 0:
                            size_frame = len(data)
                            data = data.drop(index=[size_frame-1])
                        # добавляем новые столбцы
                        data['datetime'] = pd.to_datetime(data['time'], unit='s')
                        data.index = data['datetime']
                        data['date'] = data['datetime'].dt.date
                        data['time'] = data['datetime'].dt.time
                        # print(data)

                        if first_loading_data:
                            # данных в таблице еще нет - множественная загрузка
                            logging.info('Данных в таблице еще нет - множественная загрузка строк')
                            # формируем список для загрузки в БД
                            data_load = []
                            size_frame = len(data)
                            for i in range(size_frame):
                                row = (current_ticker,
                                       '1d',
                                       datetime.combine(data['date'][i], data['time'][i]),
                                       data['open'][i],
                                       data['high'][i],
                                       data['low'][i],
                                       data['close'][i],
                                       data['close'][i],
                                       float(data['real_volume'][i]))
                                data_load.append(row)
                            # print(data_load)
                            # загружаем в БД
                            sql_obj.insert_many_rows(con, cursorObj, name_table_price_day, data_load)
                        else:
                            # данные в таблице есть - построчная загрузка с проверкой на уникальность
                            logging.info('Данные в таблице есть - построчная загрузка с проверкой на уникальность')
                            size_frame = len(data)
                            for i in range(size_frame):
                                data_load = (current_ticker,
                                             '1d',
                                             datetime.combine(data['date'][i], data['time'][i]),
                                             data['open'][i],
                                             data['high'][i],
                                             data['low'][i],
                                             data['close'][i],
                                             data['close'][i],
                                             float(data['real_volume'][i]))
                                # сделаем запрос к БД по дате и времени
                                select_date = sql_obj.select_date_time(cursorObj, name_table_price_day,
                                                                       data['date'][i],
                                                                       data['time'][i])
                                if select_date is None:
                                    # котировок с указанной датой нет. загружаем строку в БД
                                    # print(data_load)
                                    sql_obj.insert_one_rows(con, cursorObj, name_table_price_day, data_load)
                                else:
                                    # котировки с указанной датой есть
                                    pass

                    # проверим существование таблицы для загрузки недельных котировок
                    name_table_price_week = ticker_name_table + '_' + '1wk'
                    # если таблицы нет, то это будет 1-ая загрузка и ее можно выполнить путем
                    # множественной загрузки строк
                    # если таблица есть, то перед загрузкой данных нужно проверять есть ли такие данные в таблице
                    # флаг первичной загрузки данных
                    first_loading_data = False
                    if sql_obj.check_table_is_exists(cursorObj, name_table_price_week):
                        print("Таблица ", name_table_price_week, " существует")
                        logging.info('Таблица %s существует', name_table_price_week)
                    else:
                        print("Таблица ", name_table_price_week, " не обнаружена")
                        first_loading_data = True

                    # если таблицы еще нет (1-ая загрузка, получим данные c 01.01.2008 - начинается история СБЕР в
                    # терминале)
                    # если таблица уже есть, получим недостающие данные
                    bars = 0
                    # дата и время с которых нужно запросить котировки
                    timezone = pytz.timezone("Etc/UTC")
                    # дата начала интервала
                    date_from = datetime(2008, 1, 1, 0, 0, 0)
                    current_date = datetime.now()
                    # дата конца интервала - дата загрузки
                    date_to = datetime(current_date.year, current_date.month, current_date.day, 0, 0, 0)

                    if first_loading_data is False:
                        # получим последнюю строку из таблицы
                        last_row = sql_obj.get_last_row(cursorObj, name_table_price_week, "Date")
                        if last_row is not None:
                            # последняя дата в таблице
                            if last_row[0] is not None:
                                date_from = parse(last_row[0])
                                delta = timedelta(hours=168)
                                date_from = datetime(date_from.year, date_from.month, date_from.day,
                                                     0, 0, 0, tzinfo=timezone) + delta
                        else:
                            bars = 3

                    if bars != 0:
                        ticker_rates = mt5.copy_rates_from_pos(current_ticker, mt5.TIMEFRAME_W1, 0, bars)
                    else:
                        # print('type(date_from) ', type(date_from), ' date_from ', date_from)
                        # print('type(date_to) ', type(date_to), ' date_to ', date_to)
                        ticker_rates = mt5.copy_rates_range(current_ticker, mt5.TIMEFRAME_W1, date_from, date_to)
                    # создадим из полученных данных DataFrame
                    data = pd.DataFrame(ticker_rates)
                    # print(data.columns)
                    # ['time', 'open', 'high', 'low', 'close', 'tick_volume', 'spread', 'real_volume']

                    if data is None or len(data) == 0:
                        print("Не удалось прочитать недельные данные по символу ", ticker_name_table, ' или их еще нет')
                        logging.warning("Не удалось прочитать недельные данные по символу %s или их еще нет",
                                        ticker_name_table)
                    else:
                        # начало данных [0]
                        # добавляем новые столбцы
                        data['datetime'] = pd.to_datetime(data['time'], unit='s')
                        data.index = data['datetime']
                        data['date'] = data['datetime'].dt.date
                        data['time'] = data['datetime'].dt.time
                        # print(data)

                        if first_loading_data:
                            # данных в таблице еще нет - множественная загрузка
                            logging.info('Данных в таблице еще нет - множественная загрузка строк')
                            # формируем список для загрузки в БД
                            data_load = []
                            size_frame = len(data)
                            for i in range(size_frame):
                                # загружать нужно только полные недели
                                # день котировки должен быть воскресенье, полная неделя образуется в субботу
                                # поэтому между датой загрузки (текущая дата) и датой котировки должно быть не
                                # менее 6 дней
                                price_date = data['date'][i]
                                price_day = price_date.day
                                price_day_week = price_date.weekday()
                                delta = date.today() - price_date
                                if price_day_week == 6 and delta.total_seconds() >= 6 * 24 * 60 * 60:
                                    row = (current_ticker,
                                           '1wk',
                                           datetime.combine(data['date'][i], data['time'][i]),
                                           data['open'][i],
                                           data['high'][i],
                                           data['low'][i],
                                           data['close'][i],
                                           data['close'][i],
                                           float(data['real_volume'][i]))
                                    data_load.append(row)
                            # print(data_load)
                            # загружаем в БД
                            if len(data_load) > 0:
                                logging.warning('Таблица %s не обнаружена. Создадим ее', name_table_price_week)
                                # создадим таблицу для загрузки котировок
                                sql_obj.create_table_market_data(con, cursorObj, name_table_price_week)
                                sql_obj.insert_many_rows(con, cursorObj, name_table_price_week, data_load)
                        else:
                            # данные в таблице есть - построчная загрузка с проверкой на уникальность
                            logging.info('Данные в таблице есть - построчная загрузка с проверкой на уникальность')
                            size_frame = len(data)
                            for i in range(size_frame):
                                price_date = data['date'][i]
                                price_day = price_date.day
                                price_day_week = price_date.weekday()
                                delta = date.today() - price_date
                                if price_day_week == 6 and delta.total_seconds() >= 6 * 24 * 60 * 60:
                                    data_load = (current_ticker,
                                                 '1wk',
                                                 datetime.combine(data['date'][i], data['time'][i]),
                                                 data['open'][i],
                                                 data['high'][i],
                                                 data['low'][i],
                                                 data['close'][i],
                                                 data['close'][i],
                                                 float(data['real_volume'][i]))
                                    # сделаем запрос к БД по дате и времени
                                    select_date = sql_obj.select_date_time(cursorObj, name_table_price_week,
                                                                           data['date'][i],
                                                                           data['time'][i])
                                    if select_date is None:
                                        # котировок с указанной датой нет. загружаем строку в БД
                                        # print(data_load)
                                        sql_obj.insert_one_rows(con, cursorObj, name_table_price_week, data_load)
                                    else:
                                        # котировки с указанной датой есть
                                        pass
                else:
                    print('Символа ', current_ticker, ' нет в терминале МТ5')
                    logging.warning('Символа %s нет в терминале МТ5', current_ticker)
                    continue

# завершим подключение к MetaTrader 5
mt5.shutdown()

