# загрузка информации о символе и котировок в БД с Yahoo finance

import yfinance as yf
import pandas as pd
from pandas_datareader import data as pdr
import os
from datetime import datetime, date, time
from dateutil.parser import parse
from portfolio_package import sqlite_modul as sqm
import configparser
import logging


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
    global frequency_req_serv_info

    path = config.get("Settings", "path")
    name_db = config.get("Settings", "name_db")
    name_main_table = config.get("Settings", "name_main_table")
    index_ticker = int(config.get("Settings", "index_ticker"))
    frequency_req_serv_info = int(config.get("Settings", "frequency_req_serv_info"))


def convert_interval_requests(interval: str):
    if (interval == '5d'):
        return 5
    else:
        return 0


path_config_file = 'settings_expert_system.ini'   # файл с конфигурацией

# название БД
# похоже обязательно должна быть папка, просто на C: не создается
path = 'C:\DB_TEST'
name_db = 'usa_market_test.db'

# имя сводной таблицы
name_main_table = '_Tickers'

# индекс тикера с которого начинать загрузку (=1 - первый тикер в сводной таблице)
index_ticker = 1
# задержка между запросами
frequency_req_serv_info = 5

####################################################################################################
# загрузка параметров из файла конфигурации
crud_config(path_config_file)
print('Load settings from configuration files: ')
print('path: ', path)
print('name_db : ', name_db)
print('name_main_table : ', name_main_table)
print('index_ticker : ', index_ticker)
print('frequency_req_serv_info : ', frequency_req_serv_info)

logging.basicConfig(filename='yahoo_agent_quotes.log',
                    format='[%(asctime)s] [%(levelname)s] => %(message)s',
                    filemode='w',
                    level=logging.DEBUG)

filename = path + '\\' + name_db
print('Используется БД: ', filename)
logging.info('Используется БД: %s', filename)

sql_obj = sqm.SQLite()

# основной цикл, предполагается однократный запуск с помощью планировщика
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

                # получим данные как dataframe за последние 5 дней
                timeframe = '1h'
                interval_request = '5d'
                try:
                    # ускоренная загрузка
                    # yf.pdr_override()
                    # data_ = pdr.get_data_yahoo(current_ticker, interval=timeframe, period=interval_request)
                    # ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
                    # обычная загрузка
                    data_ = yf.download(tickers=current_ticker, interval=timeframe, period=interval_request,
                                        actions=True, threads=False)
                    # ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume', 'Dividends', 'Stock Splits']
                except:
                    print("Не удалось получить котировки по символу ", current_ticker)
                    logging.warning("Не удалось получить котировки по символу %s", current_ticker)
                    continue

                if data_ is None or len(data_) == 0:
                    print("Не удалось прочитать данные по символу ", current_ticker)
                    logging.warning("Не удалось прочитать данные по символу %s", current_ticker)
                    continue
                else:
                    # print(data_)
                    # print(data_.columns)
                    # data_.to_csv('primer')
                    logging.info('Получены часовые котировки. Значение ячейки [0,0] = %d', data_.iloc[0, 0])

                    # нужно обрезать строки где минуты не соответствуют таймфрейму
                    # результат будет в новом фрейме
                    data = pd.DataFrame()
                    data_['datetime'] = data_.index
                    data_['minute'] = data_['datetime'].dt.minute
                    # для часовых данных с американского рынка, правильное значение минут
                    correct_minute = 30
                    size_frame_ = len(data_)
                    for i in range(size_frame_):
                        # для ускоренной загрузки минуты в колонке 7
                        # if data_.iloc[i, 7] % correct_minute == 0:
                        # для ускоренной загрузки минуты в колонке 9
                        if data_.iloc[i, 9] % correct_minute == 0:
                            # print("правильная строка")
                            data = data.append(data_.iloc[i,], ignore_index=True)
                        else:
                            # print("не правильная строка со значением: ", data_.iloc[i, 7])
                            pass
                    # print(data.columns)

                    # добавляем новые столбцы
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
                                   data['Open'][i],
                                   data['High'][i],
                                   data['Low'][i],
                                   data['Close'][i],
                                   data['Adj Close'][i],
                                   float(data['Volume'][i]))
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
                                   data['Open'][i],
                                   data['High'][i],
                                   data['Low'][i],
                                   data['Close'][i],
                                   data['Adj Close'][i],
                                   float(data['Volume'][i]))
                            # сделаем запрос к БД по дате и времени
                            select_date = sql_obj.select_date_time(cursorObj, name_table_price_hour, data['date'][i],
                                                                   data['time'][i])
                            if select_date is None:
                                # котировок с указанной датой нет. загружаем строку в БД
                                # print(data_load)
                                sql_obj.insert_one_rows(con, cursorObj, name_table_price_hour, data_load)
                            else:
                                # котировки с указанной датой есть
                                pass

                    # проверим наличие таблицы дневных котировок
                    name_table_price_day = current_ticker + '_' + '1d'
                    if sql_obj.check_table_is_exists(cursorObj, name_table_price_day):
                        print("Таблица ", name_table_price_day, " существует")
                        logging.info('Таблица %s существует', name_table_price_day)
                        # получим последнюю строку из таблицы
                        last_row = sql_obj.get_last_row(cursorObj, name_table_price_day, "Date")
                        # print(last_row)
                        if last_row is not None:
                            # последняя дата в таблице
                            if last_row[0] is not None:
                                last_date = parse(last_row[0])
                                last_date_ = last_date.date()
                                # 1-ая дата в массиве часовых котировок
                                # для обычной загрузки
                                # ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume', 'Dividends',
                                #  'Stock Splits', 'datetime', 'minute', 'date', 'time']
                                # print(data.columns)
                                # для ускоренной загрузки
                                # first_date_ = data.iloc[0, 8]
                                # для обычной загрузки
                                first_date_ = data.iloc[0, 10]
                                delta = first_date_ - last_date_
                                if int(delta.days) < convert_interval_requests(interval_request):
                                    # дневные и часовые котировки перекрываются
                                    # print("Для тикера ", current_ticker, " дневные и часовые котировки перекрываются")
                                    logging.info("Для тикера %s дневные и часовые котировки перекрываются",
                                                 current_ticker)
                                    pass
                                else:
                                    print("Для тикера ", current_ticker, " дневные и часовые котировки не перекрываются.",
                                          " Нужно запустить yahoo_agent_info для этого тикера")
                                    logging.warning("Для тикера %s дневные и часовые котировки не перекрываются",
                                                    current_ticker)
                            else:
                                print("Для тикера ", current_ticker, " последняя дата == None")
                                logging.warning("Для тикера %s последняя дата == None", current_ticker)

                            # формируем из часовых данных - дневные
                            del data['datetime']
                            del data['date']
                            del data['time']
                            del data['minute']
                            quotes_ohlc = data.resample('1d').ohlc()
                            quotes_ohlc['datetime'] = quotes_ohlc.index
                            quotes_ohlc['date'] = quotes_ohlc['datetime'].dt.date
                            quotes_ohlc['time'] = quotes_ohlc['datetime'].dt.time
                            # print(quotes_ohlc.columns)
                            # print(quotes_ohlc)
                            quotes_sum = data.resample('1d').sum()
                            # print(quotes_sum.columns)
                            # print(quotes_sum)

                            size_frame = len(quotes_ohlc)
                            for i in range(size_frame):
                                # формируем строку для загрузки в БД
                                open_d = quotes_ohlc.iloc[i, 0]
                                high_d = quotes_ohlc.iloc[i, 5]
                                low_d = quotes_ohlc.iloc[i, 10]
                                close_d = quotes_ohlc.iloc[i, 15]
                                volume_d = float(quotes_sum.iloc[i, 5])
                                # при агрегировании добавляются выходные дни с нулевыми значениями
                                if open_d > 0 and high_d > 0 and low_d > 0 and close_d > 0:
                                    row_market_data = (current_ticker,
                                                       '1d',
                                                       datetime.combine(quotes_ohlc['date'][i], quotes_ohlc['time'][i]),
                                                       open_d,
                                                       high_d,
                                                       low_d,
                                                       close_d,
                                                       close_d,
                                                       volume_d)
                                    # получим строку с котировками из БД по дате и времени
                                    select_date = sql_obj.select_date_time(cursorObj, name_table_price_day,
                                                                           quotes_ohlc['date'][i],
                                                                           quotes_ohlc['time'][i])
                                    if select_date is None:
                                        # котировок с указанной датой нет. загружаем строку в БД
                                        # print(data_load)
                                        sql_obj.insert_one_rows(con, cursorObj, name_table_price_day, row_market_data)
                                    else:
                                        # котировки с указанной датой есть

                                        # print(select_date)
                                        # print("Котировки с указанной датой ", str(quotes_ohlc['date'][i]),
                                        #       " и временем ", str(quotes_ohlc['time'][i]), " для тикера ",
                                        #       current_ticker, " есть")

                                        # если в строке есть значения None: удаляем текущую строку и записываем новую
                                        data_correct = True
                                        size_frame_ = len(select_date)
                                        for j in range(size_frame_):
                                            if select_date[j] is None or select_date[j] == 0 or select_date[j] == '':
                                                data_correct = False
                                                break
                                        if data_correct is False:
                                            print("Обнаружены невалидные значения для тикера ", current_ticker)
                                            logging.warning("Обнаружены невалидные значения для тикера %s",
                                                            current_ticker)
                                            sql_obj.delete_rows_datetime_condition(con,
                                                                                   cursorObj,
                                                                                   name_table_price_day,
                                                                                   'Date',
                                                                                   str(quotes_ohlc['date'][i]),
                                                                                   str(quotes_ohlc['time'][i]))
                                            sql_obj.insert_one_rows(con, cursorObj, name_table_price_day,
                                                                    row_market_data)

                                        # если котировки для текущей даты, то нужно обновить строку
                                        current_date = datetime.now()
                                        current_date_ = current_date.date()
                                        if current_date_ == quotes_ohlc['date'][i]:
                                            sql_obj.delete_rows_datetime_condition(con,
                                                                                   cursorObj,
                                                                                   name_table_price_day,
                                                                                   'Date',
                                                                                   str(quotes_ohlc['date'][i]),
                                                                                   str(quotes_ohlc['time'][i]))
                                            sql_obj.insert_one_rows(con, cursorObj, name_table_price_day,
                                                                    row_market_data)
                        else:
                            print("Не проходит запрос Последняя дата в Таблице ", name_table_price_day)
                            logging.warning("Не проходит запрос Последняя дата в Таблице %s",
                                            name_table_price_day)
                    else:
                        print("Таблица ", name_table_price_day, " не существует. Нужно запустить",
                              " yahoo_agent_info для этого тикера")
                        logging.warning("Таблица %s не существует", name_table_price_day)
    else:
        print("Таблица ", name_main_table, " не существует. Нужно создать ее вручную. Завершение работы")
        logging.warning("Таблица %s не существует. Нужно создать ее вручную.", name_main_table)

    # проверим существование сводной таблицы Market
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
                # в тикерах индексов Yahoo есть символ ^, который не воспринимается БД
                # поэтому в качестве тикера для создания таблиц используем колонку 'Ticker_DB'
                current_ticker = row[1]
                ticker_name_table = row[6]
                # print(row[1])
                logging.info('Текущий тикер %s', ticker_name_table)
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

                # получим данные как dataframe за последние 5 дней
                timeframe = '1h'
                interval_request = '5d'
                try:
                    # ускоренная загрузка
                    # yf.pdr_override()
                    # data_ = pdr.get_data_yahoo(current_ticker, interval=timeframe, period=interval_request)
                    # ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
                    # обычная загрузка
                    data_ = yf.download(tickers=current_ticker, interval=timeframe, period=interval_request,
                                        actions=True, threads=False)
                    # ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume', 'Dividends', 'Stock Splits']
                except:
                    print("Не удалось получить котировки по символу ", ticker_name_table)
                    logging.warning("Не удалось получить котировки по символу %s", ticker_name_table)
                    continue

                if data_ is None or len(data_) == 0:
                    print("Не удалось прочитать данные по символу ", ticker_name_table)
                    logging.warning("Не удалось прочитать данные по символу %s", ticker_name_table)
                    continue
                else:
                    # print(data_)
                    # print(data_.columns)
                    # data_.to_csv('primer')
                    logging.info('Получены часовые котировки. Значение ячейки [0,0] = %d', data_.iloc[0, 0])

                    # нужно обрезать строки где минуты не соответствуют таймфрейму
                    # результат будет в новом фрейме
                    data = pd.DataFrame()
                    data_['datetime'] = data_.index
                    data_['minute'] = data_['datetime'].dt.minute
                    # для часовых данных с американского рынка, правильное значение минут
                    correct_minute = 30
                    size_frame_ = len(data_)
                    for i in range(size_frame_):
                        # для ускоренной загрузки минуты в колонке 7
                        # if data_.iloc[i, 7] % correct_minute == 0:
                        # для ускоренной загрузки минуты в колонке 9
                        if data_.iloc[i, 9] % correct_minute == 0:
                            # print("правильная строка")
                            data = data.append(data_.iloc[i, ], ignore_index=True)
                        else:
                            # print("не правильная строка со значением: ", data_.iloc[i, 7])
                            pass
                    # print(data.columns)

                    # добавляем новые столбцы
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
                            row = (ticker_name_table,
                                   '1h',
                                   datetime.combine(data['date'][i], data['time'][i]),
                                   data['Open'][i],
                                   data['High'][i],
                                   data['Low'][i],
                                   data['Close'][i],
                                   data['Adj Close'][i],
                                   float(data['Volume'][i]))
                            data_load.append(row)
                        # print(data_load)
                        # загружаем в БД
                        sql_obj.insert_many_rows(con, cursorObj, name_table_price_hour, data_load)
                    else:
                        # данные в таблице есть - построчная загрузка с проверкой на уникальность
                        logging.info('Данные в таблице есть - построчная загрузка с проверкой на уникальность')
                        size_frame = len(data)
                        for i in range(size_frame):
                            data_load = (ticker_name_table,
                                   '1h',
                                   datetime.combine(data['date'][i], data['time'][i]),
                                   data['Open'][i],
                                   data['High'][i],
                                   data['Low'][i],
                                   data['Close'][i],
                                   data['Adj Close'][i],
                                   float(data['Volume'][i]))
                            # сделаем запрос к БД по дате и времени
                            select_date = sql_obj.select_date_time(cursorObj, name_table_price_hour, data['date'][i],
                                                                   data['time'][i])
                            if select_date is None:
                                # котировок с указанной датой нет. загружаем строку в БД
                                # print(data_load)
                                sql_obj.insert_one_rows(con, cursorObj, name_table_price_hour, data_load)
                            else:
                                # котировки с указанной датой есть
                                pass

                    # проверим наличие таблицы дневных котировок
                    name_table_price_day = ticker_name_table + '_' + '1d'
                    if sql_obj.check_table_is_exists(cursorObj, name_table_price_day):
                        print("Таблица ", name_table_price_day, " существует")
                        logging.info('Таблица %s существует', name_table_price_day)
                        # получим последнюю строку из таблицы
                        last_row = sql_obj.get_last_row(cursorObj, name_table_price_day, "Date")
                        # print(last_row)
                        if last_row is not None:
                            # последняя дата в таблице
                            if last_row[0] is not None:
                                last_date = parse(last_row[0])
                                last_date_ = last_date.date()
                                # 1-ая дата в массиве часовых котировок
                                # для обычной загрузки
                                # ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume', 'Dividends',
                                #  'Stock Splits', 'datetime', 'minute', 'date', 'time']
                                # print(data.columns)
                                # для ускоренной загрузки
                                # first_date_ = data.iloc[0, 8]
                                # для обычной загрузки
                                first_date_ = data.iloc[0, 10]
                                delta = first_date_ - last_date_
                                if int(delta.days) < convert_interval_requests(interval_request):
                                    # дневные и часовые котировки перекрываются
                                    # print("Для тикера ", ticker_name_table,
                                    # " дневные и часовые котировки перекрываются")
                                    logging.info("Для тикера %s дневные и часовые котировки перекрываются",
                                                 ticker_name_table)
                                    pass
                                else:
                                    print("Для тикера ", ticker_name_table,
                                          " дневные и часовые котировки не перекрываются.",
                                          " Нужно запустить yahoo_agent_info для этого тикера")
                                    logging.warning("Для тикера %s дневные и часовые котировки не перекрываются",
                                                    ticker_name_table)
                            else:
                                print("Для тикера ", ticker_name_table, " последняя дата == None")
                                logging.warning("Для тикера %s последняя дата == None", ticker_name_table)

                            # формируем из часовых данных - дневные
                            del data['datetime']
                            del data['date']
                            del data['time']
                            del data['minute']
                            quotes_ohlc = data.resample('1d').ohlc()
                            quotes_ohlc['datetime'] = quotes_ohlc.index
                            quotes_ohlc['date'] = quotes_ohlc['datetime'].dt.date
                            quotes_ohlc['time'] = quotes_ohlc['datetime'].dt.time
                            # print(quotes_ohlc.columns)
                            # print(quotes_ohlc)
                            quotes_sum = data.resample('1d').sum()
                            # print(quotes_sum.columns)
                            # print(quotes_sum)

                            size_frame = len(quotes_ohlc)
                            for i in range(size_frame):
                                # формируем строку для загрузки в БД
                                open_d = quotes_ohlc.iloc[i, 0]
                                high_d = quotes_ohlc.iloc[i, 5]
                                low_d = quotes_ohlc.iloc[i, 10]
                                close_d = quotes_ohlc.iloc[i, 15]
                                volume_d = float(quotes_sum.iloc[i, 5])
                                # при агрегировании добавляются выходные дни с нулевыми значениями
                                if open_d > 0 and high_d > 0 and low_d > 0 and close_d > 0:
                                    row_market_data = (ticker_name_table,
                                                       '1d',
                                                       datetime.combine(quotes_ohlc['date'][i], quotes_ohlc['time'][i]),
                                                       open_d,
                                                       high_d,
                                                       low_d,
                                                       close_d,
                                                       close_d,
                                                       volume_d)
                                    # получим строку с котировками из БД по дате и времени
                                    select_date = sql_obj.select_date_time(cursorObj, name_table_price_day,
                                                                           quotes_ohlc['date'][i],
                                                                           quotes_ohlc['time'][i])
                                    if select_date is None:
                                        # котировок с указанной датой нет. загружаем строку в БД
                                        # print(data_load)
                                        sql_obj.insert_one_rows(con, cursorObj, name_table_price_day, row_market_data)
                                    else:
                                        # котировки с указанной датой есть

                                        # print(select_date)
                                        # print("Котировки с указанной датой ", str(quotes_ohlc['date'][i]),
                                        #       " и временем ", str(quotes_ohlc['time'][i]), " для тикера ",
                                        #       ticker_name_table, " есть")

                                        # если в строке есть значения None: удаляем текущую строку и записываем новую
                                        data_correct = True
                                        size_frame_ = len(select_date)
                                        for j in range(size_frame_):
                                            # на равно 0 не проверяем, обьем у индексов может быть нулевой
                                            if select_date[j] is None or select_date[j] == '':
                                                data_correct = False
                                                break
                                        if data_correct is False:
                                            print("Обнаружены невалидные значения для тикера ", ticker_name_table)
                                            logging.warning("Обнаружены невалидные значения для тикера %s",
                                                            ticker_name_table)
                                            sql_obj.delete_rows_datetime_condition(con,
                                                                                   cursorObj,
                                                                                   name_table_price_day,
                                                                                   'Date',
                                                                                   str(quotes_ohlc['date'][i]),
                                                                                   str(quotes_ohlc['time'][i]))
                                            sql_obj.insert_one_rows(con, cursorObj, name_table_price_day,
                                                                    row_market_data)

                                        # если котировки для текущей даты, то нужно обновить строку
                                        current_date = datetime.now()
                                        current_date_ = current_date.date()
                                        if current_date_ == quotes_ohlc['date'][i]:
                                            sql_obj.delete_rows_datetime_condition(con,
                                                                                   cursorObj,
                                                                                   name_table_price_day,
                                                                                   'Date',
                                                                                   str(quotes_ohlc['date'][i]),
                                                                                   str(quotes_ohlc['time'][i]))
                                            sql_obj.insert_one_rows(con, cursorObj, name_table_price_day,
                                                                    row_market_data)
                        else:
                            print("Не проходит запрос Последняя дата в Таблице ", name_table_price_day)
                            logging.warning("Не проходит запрос Последняя дата в Таблице %s",
                                            name_table_price_day)
                    else:
                        print("Таблица ", name_table_price_day, " не существует. Нужно запустить",
                              " yahoo_agent_info для этого тикера")
                        logging.warning("Таблица %s не существует", name_table_price_day)
    else:
        print("Таблица ", '_Market', " не существует. Нужно создать ее вручную. Завершение работы")
        logging.warning("Таблица %s не существует. Нужно создать ее вручную.", '_Market')

    con.close()
    quit()