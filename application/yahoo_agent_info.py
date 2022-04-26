# загрузка информации о символе в БД с Yahoo finance

import yfinance as yf
import pandas as pd
import os
from datetime import datetime, date, time, timedelta
from time import sleep
from portfolio_package import sqlite_modul as sqm
import configparser
import logging

name_program = 'yahoo_agent_info'
comment_prog = 'получение дневных котировок за 2 года, дивидендов, историии рекоммендаций с сайта finance.yahoo.com'

path_config_file = 'settings_expert_system.ini'   # файл с конфигурацией

# название БД
path = 'C:\DB_TEST'
name_db = 'usa_market_test.db'

# имя сводной таблицы
name_main_table = '_Tickers'

# префикс символа справа
prefix_on_right = '.ME'

# индекс тикера с которого начинать загрузку (=1 - первый тикер в сводной таблице)
index_ticker = 1
# индекс тикера на котором заканчивать загрузку
index_ticker_end = 10000

# задержка между запросами
frequency_req_serv_info = 5

# расписание загрузок
# 0-пн-к, 1-вт-к, 2-среда, 3-четв, 4-пт-ца, 5-сб-та, 6-вск
day_load_info = 1
start_load_info_hour = 5
start_load_info_min = 0


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
    global path_config_file
    global path
    global name_db
    global index_ticker
    global index_ticker_end
    global frequency_req_serv_info
    global day_load_info
    global start_load_info_hour
    global start_load_info_min
    global prefix_on_right

    path = config.get("Settings", "path")
    name_db = config.get("Settings", "name_db")
    index_ticker = int(config.get("Settings", "index_ticker"))
    index_ticker_end = int(config.get("Settings", "index_ticker_end"))
    frequency_req_serv_info = int(config.get("Settings", "frequency_req_serv_info"))
    day_load_info = int(config.get("Settings", "day_load_info"))
    start_load_info_hour = int(config.get("Settings", "start_load_info_hour"))
    start_load_info_min = int(config.get("Settings", "start_load_info_min"))
    prefix_on_right = config.get("Settings", "prefix_on_right")


def raiting_calculation(raiting_list: list):
    # веса
    strong_buy = 1
    buy = 2
    outperform = 2.5
    hold = 3
    underperform = 3.5
    sell = 4
    strong_sell = 5

    count = 0
    summ = 0
    size_frame = len(raiting_list)
    for i in range(size_frame):
        if raiting_list[i] == 'Strong Buy':
            summ = summ + strong_buy
            count = count + 1
        elif raiting_list[i] == 'Buy' or raiting_list[i] == 'Long-Term Buy':
            summ = summ + buy
            count = count + 1
        elif raiting_list[i] == 'Outperform' or raiting_list[i] == 'Overweight':
            summ = summ + outperform
            count = count + 1
        elif raiting_list[i] == 'Hold' or raiting_list[i] == 'Market Perform' or raiting_list[i] == 'Neutral' or raiting_list[i] == 'Equal-Weight':
            summ = summ + hold
            count = count + 1
        elif raiting_list[i] == 'Underperform' or raiting_list[i] == 'Underweight':
            summ = summ + underperform
            count = count + 1
        elif raiting_list[i] == 'Sell':
            summ = summ + sell
            count = count + 1
        elif raiting_list[i] == 'Strong Sell':
            summ = summ + strong_sell
            count = count + 1
        else:
            pass
    if count == 0:
        return 0
    else:
        return round(summ/count, 1)


def set_interval_recommendation():
    current_date = datetime.now()
    year = current_date.year
    month = current_date.month
    start_month = 0
    start_year = 0
    main_month = 0
    main_year = 0

    # на основании текущей даты определяем начало интервала запрашиваемых данных
    # для худшего сценария (когда оценок мало, или их еще нет за последний отчетный период)
    # 4 квартал + годовой отчет, обновление оценок после 20-х чисел января
    if month == 1:
        start_month = 10
        start_year = year - 1
        main_month = 1
        main_year = year
    elif month == 2:
        start_month = 10
        start_year = year - 1
        main_month = 1
        main_year = year
    elif month == 3:
        start_month = 10
        start_year = year - 1
        main_month = 1
        main_year = year
    # 2 квартал, обновление оценок после 20-х чисел апреля
    elif month == 4:
        start_month = 1
        start_year = year
        main_month = 4
        main_year = year
    elif month == 5:
        start_month = 1
        start_year = year
        main_month = 4
        main_year = year
    elif month == 6:
        start_month = 1
        start_year = year
        main_month = 4
        main_year = year
    # 3 квартал, обновление оценок после 20-х чисел июля
    elif month == 7:
        start_month = 4
        start_year = year
        main_month = 7
        main_year = year
    elif month == 8:
        start_month = 4
        start_year = year
        main_month = 7
        main_year = year
    elif month == 9:
        start_month = 4
        start_year = year
        main_month = 7
        main_year = year
    # 4 квартал, обновление оценок после 20-х чисел октября
    elif month == 10:
        start_month = 7
        start_year = year
        main_month = 10
        main_year = year
    elif month == 11:
        start_month = 7
        start_year = year
        main_month = 10
        main_year = year
    elif month == 12:
        start_month = 7
        start_year = year
        main_month = 10
        main_year = year
    else:
        pass

    return [start_month, start_year, main_month, main_year]


####################################################################################################
# загрузка параметров из файла конфигурации
crud_config(path_config_file)
print('Load settings from configuration files: ')
print('path: ', path)
print('name_db : ', name_db)
print('index_ticker : ', index_ticker)
print('index_ticker_end : ', index_ticker_end)
print('prefix_on_right : ', prefix_on_right)
print('frequency_req_serv_info : ', frequency_req_serv_info)
print('day_load_info : ', day_load_info)
print('start_load_info_hour : ', start_load_info_hour)
print('start_load_info_min : ', start_load_info_min)

logging.basicConfig(filename='yahoo_agent_info.log',
                    format='[%(asctime)s] [%(levelname)s] => %(message)s',
                    filemode='w',
                    level=logging.DEBUG)

filename = path + '\\' + name_db
print('Используется БД: ', filename)
logging.info('Используется БД: %s', filename)

sql_obj = sqm.SQLite()

# основной цикл, предполагается однократный запуск с помощью планировщика
if 1 == 1:
    # проверка наличия БД
    if sql_obj.check_db(filename) is False:
        print("База данных ", filename, " не обнаружена. Завершение работы")
        logging.warning('База данных %s не обнаружена', filename)
        quit()
    else:
        print("База данных ", filename, " существует")
        logging.info('База данных %s существует', filename)

    # устанавливаем соединение и создаем обьект курсора
    con = sql_obj.create_connection(filename)
    cursorObj = con.cursor()

    # проверим существование системных таблиц
    control_table_sys_log = sql_obj.check_table_is_exists(cursorObj, '_System_log')
    if control_table_sys_log is False:
        print('Таблицы _System_log не существует. Создадим ее')
        logging.warning('Таблицы %s не существует. Создадим ее', '_System_log')
        q = """
        CREATE TABLE {table} (
        Date_event date DEFAULT '1970.01.01', 
        Name_program text DEFAULT '', 
        Comment text DEFAULT '', 
        Code_finish integer DEFAULT 0.0
        )
        """
        cursorObj.execute(q.format(table='_System_log'))
        con.commit()

    if 1 == 1:
        current_date = datetime.now()
        day_week = current_date.weekday()
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
                # так же проверяем статус тикера (листинг/делистинг)
                if (row[0] >= index_ticker) and (row[0] <= index_ticker_end) and (row[10] == 'Yes'):
                    current_ticker = row[1]
                    logging.info('Текущий тикер %s', current_ticker)
                    ticker_obj = yf.Ticker(current_ticker + prefix_on_right)
                    # получим информацию
                    ticker_obj.info

                    # если нет таблицы дневных котировок и есть данные,
                    # создадим таблицу и закачаем данные
                    name_table_price_day = current_ticker + '_' + '1d'
                    if sql_obj.check_table_is_exists(cursorObj, name_table_price_day):
                        print("Таблица ", name_table_price_day, " существует")
                        logging.info('Таблица %s существует', name_table_price_day)
                        # ничего не делаем, будем ее заполнять на этапе получения часовых данных
                        pass
                    else:
                        # получим котировки за последние 2 года
                        data = ticker_obj.history(period="2y")
                        if data is None or len(data) == 0:
                            pass
                        else:
                            # print(data)
                            print("Таблица ", name_table_price_day, " не обнаружена")
                            logging.warning('Таблица %s не обнаружена', name_table_price_day)

                            # добавляем новые столбцы
                            data['datetime'] = data.index
                            data['date'] = data['datetime'].dt.date
                            data['time'] = data['datetime'].dt.time

                            # формируем список для загрузки в БД
                            data_load = []
                            size_frame = len(data)
                            for i in range(size_frame):
                                row_market_data = (current_ticker,
                                                   '1d',
                                                   datetime.combine(data['date'][i], data['time'][i]),
                                                   round(data['Open'][i], 2),
                                                   round(data['High'][i], 2),
                                                   round(data['Low'][i], 2),
                                                   round(data['Close'][i], 2),
                                                   round(data['Close'][i], 2),
                                                   float(data['Volume'][i]))
                                data_load.append(row_market_data)
                            # print(data_load)
                            if len(data_load) > 1:
                                # создадим таблицу для загрузки котировок
                                sql_obj.create_table_market_data(con, cursorObj, name_table_price_day)
                                # загружать нужно только полные дни. если загрузка происходит в будний день
                                # обрезаем последнюю строку в списке
                                if day_week == 5 or day_week == 6:
                                    sql_obj.insert_many_rows(con, cursorObj, name_table_price_day, data_load)
                                else:
                                    size_list = len(data_load)
                                    del data_load[size_list - 1]
                                    sql_obj.insert_many_rows(con, cursorObj, name_table_price_day, data_load)

                    # если нет таблицы недельных котировок и есть данные,
                    # создадим таблицу и закачаем данные
                    name_table_price_week = current_ticker + '_' + '1wk'
                    if sql_obj.check_table_is_exists(cursorObj, name_table_price_week):
                        print("Таблица ", name_table_price_week, " существует")
                        logging.info('Таблица %s существует', name_table_price_week)
                        # ничего не делаем, будем ее заполнять на этапе получения часовых данных
                        pass
                    else:
                        # получим котировки за последние 10 лет
                        data = ticker_obj.history(period="10y", interval='1wk')
                        if data is None or len(data) == 0:
                            pass
                        else:
                            # print(data)
                            print("Таблица ", name_table_price_week, " не обнаружена")
                            logging.warning('Таблица %s не обнаружена', name_table_price_week)

                            # добавляем новые столбцы
                            data['datetime'] = data.index
                            data['date'] = data['datetime'].dt.date
                            data['time'] = data['datetime'].dt.time

                            # формируем список для загрузки в БД
                            data_load = []
                            size_frame = len(data)
                            for i in range(size_frame):
                                # загружать нужно только полные недели
                                # день котировки должен быть понедельник, полная неделя образуется в субботу
                                # поэтому между датой загрузки (текущая дата) и датой котировки должно быть не
                                # менее 6 дней
                                price_date = data['date'][i]
                                price_day = price_date.day
                                price_day_week = price_date.weekday()
                                delta = date.today() - price_date
                                if price_day_week == 0 and delta.total_seconds() >= 6 * 24 * 60 * 60:
                                    if data['Close'][i] is not None and data['Close'][i] > 0:
                                        row_market_data = (current_ticker,
                                                           '1wk',
                                                           datetime.combine(data['date'][i], data['time'][i]),
                                                           round(data['Open'][i], 2),
                                                           round(data['High'][i], 2),
                                                           round(data['Low'][i], 2),
                                                           round(data['Close'][i], 2),
                                                           round(data['Close'][i], 2),
                                                           float(data['Volume'][i]))
                                        data_load.append(row_market_data)

                            if len(data_load) > 0:
                                # создадим таблицу для загрузки котировок
                                sql_obj.create_table_market_data(con, cursorObj, name_table_price_week)
                                sql_obj.insert_many_rows(con, cursorObj, name_table_price_week, data_load)

                    # добавим isin_code в сводную таблицу, если его там еще нет
                    isin_code = ticker_obj.isin
                    if isin_code is None:
                        pass
                    else:
                        find_value = (isin_code,)
                        q = "SELECT ISIN_code FROM {table} WHERE ISIN_code=?"
                        q_mod = q.format(table=name_main_table)
                        cursorObj.execute(q_mod, find_value)
                        if cursorObj.fetchone() is None:
                            q = 'UPDATE {table} SET ISIN_code = "{new_value}" WHERE Ticker=?'
                            q_mod = q.format(table=name_main_table, new_value=isin_code)
                            # print(q_mod)
                            cursorObj.execute(q_mod, (current_ticker,))
                            con.commit()
                        else:
                            # isin_code уже есть в сводной таблице
                            pass

                    # обновим информацию по дивидендам
                    # получаем Pandas Series с информацией
                    show_dividends = ticker_obj.dividends
                    # проверим существование таблицы для загрузки
                    name_table_dividends = current_ticker + '_Dividends'
                    if sql_obj.check_table_is_exists(cursorObj, name_table_dividends):
                        print("Таблица ", name_table_dividends, " существует")
                        logging.info('Таблица %s существует', name_table_dividends)
                    else:
                        # таблицы не существует. создадим ее, если есть данные
                        if show_dividends is None or len(show_dividends) == 0:
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
                    if show_dividends is None or len(show_dividends) == 0:
                        pass
                    else:
                        if sql_obj.check_table_is_exists(cursorObj, name_table_dividends):
                            # запускаем цикл по Series
                            size_frame = len(show_dividends)
                            for i in range(size_frame):
                                q = '''INSERT INTO {table} (Ticker, Payment_date, Dividends)
                                        VALUES(?, ?, ?)'''
                                q_mod = q.format(table=name_table_dividends)

                                # перед загрузкой проверим дату на уникальность для текущей таблицы
                                # преобразование timestamp в datetime
                                d = str(show_dividends.index[i])
                                d_mod = datetime.strptime(d[0:10], "%Y-%m-%d")
                                # если значение не найдено, записываем его
                                find_value = (d_mod,)
                                q_1 = "SELECT {column_1} FROM {table} WHERE {column_1}=?"
                                q_1_mod = q_1.format(table=name_table_dividends, column_1='Payment_date')
                                cursorObj.execute(q_1_mod, find_value)
                                if cursorObj.fetchone() is None:
                                    # заносим в таблицу данные
                                    cursorObj.execute(q_mod, (current_ticker, d_mod, show_dividends[i]))
                                    con.commit()
                        else:
                            print("Таблица ", name_table_dividends, " не существует")
                            logging.warning('Таблица %s не существует', name_table_dividends)

                    # обновим рекомендации
                    # получаем Pandas DataFrame с информацией
                    analysts_recommendations = ticker_obj.recommendations
                    # проверим существование таблицы для загрузки
                    name_table_recommendation = current_ticker + '_Recommendations'
                    if sql_obj.check_table_is_exists(cursorObj, name_table_recommendation):
                        print("Таблица ", name_table_recommendation, " существует")
                        logging.info('Таблица %s существует', name_table_recommendation)
                    else:
                        # таблицы не существует. создадим ее если есть данные
                        if analysts_recommendations is None or len(analysts_recommendations) == 0:
                            pass
                        else:
                            logging.warning('Таблица %s не обнаружена. Создадим ее', name_table_recommendation)
                            q = """ CREATE TABLE {table}
                            (
                            Ticker text NOT NULL,
                            Date_recommend date DEFAULT '1970.01.01',
                            Firm text DEFAULT '',
                            Grade text DEFAULT '',
                            From_grade text DEFAULT '',
                            Action text DEFAULT ''
                            )
                            """
                            q_mod = q.format(table=name_table_recommendation)
                            # print(q_mod)
                            cursorObj.execute(q_mod)
                            con.commit()

                    # заполняем таблицу
                    if analysts_recommendations is None or len(analysts_recommendations) == 0:
                        pass
                    else:
                        if sql_obj.check_table_is_exists(cursorObj, name_table_recommendation):
                            size_frame = len(analysts_recommendations)
                            for i in range(size_frame):
                                q = '''INSERT INTO {table} (Ticker, Date_recommend, Firm, Grade, From_grade, Action)
                                        VALUES(?, ?, ?, ?, ?, ?)'''
                                q_mod = q.format(table=name_table_recommendation)

                                # перед загрузкой проверим дату на уникальность для текущей таблицы
                                # преобразование timestamp в datetime
                                d = str(analysts_recommendations.index[i])
                                d_mod = datetime.strptime(d, "%Y-%m-%d %H:%M:%S")
                                # если значение не найдено, записываем его
                                find_value = (d_mod,)
                                q_1 = "SELECT {column_1} FROM {table} WHERE {column_1}=?"
                                q_1_mod = q_1.format(table=name_table_recommendation, column_1='Date_recommend')
                                cursorObj.execute(q_1_mod, find_value)
                                if cursorObj.fetchone() is None:
                                    # заносим в таблицу данные
                                    cursorObj.execute(q_mod, (current_ticker, d_mod,
                                                              analysts_recommendations['Firm'][i],
                                                              analysts_recommendations['To Grade'][i],
                                                              analysts_recommendations['From Grade'][i],
                                                              analysts_recommendations['Action'][i]))
                                    con.commit()

                            # на основании загруженных данных рассчитаем оценку и внесем ее в сводную
                            # таблицу с датой обновления
                            # определим начало интервала для расчета оценки
                            result = set_interval_recommendation()
                            start_month = result[0]
                            start_year = result[1]
                            start_day = 1
                            # ключевой месяц для расчета оценки
                            main_month = result[2]
                            main_year = result[3]
                            start_time = time(0, 0)

                            main_date = date(main_year, main_month, start_day)
                            main_date_ = datetime.combine(main_date, start_time)
                            start_date = date(start_year, start_month, start_day)

                            # организуем запрос к БД
                            find_date = datetime.combine(start_date, start_time)
                            find_value = (find_date,)
                            q_1 = "SELECT * FROM {table} WHERE {column_1} >= ?"
                            q_1_mod = q_1.format(table=name_table_recommendation, column_1='Date_recommend')
                            cursorObj.execute(q_1_mod, find_value)
                            select_result = cursorObj.fetchall()

                            # расчет оценки
                            if select_result is None:
                                print("Оценки аналитиков для ", current_ticker, " не найдены")
                                logging.info('Оценки аналитиков для %s не найдены', current_ticker)
                            else:
                                # сделаем 2 списка: дата и оценка с нужными преобразованиями
                                count_raiting = 0
                                date_raiting = []
                                raiting = []
                                size_frame = len(select_result)
                                for i in range(size_frame):
                                    date_raiting.append(datetime.strptime(select_result[i][1], "%Y-%m-%d %H:%M:%S"))
                                    if datetime.strptime(select_result[i][1], "%Y-%m-%d %H:%M:%S") >= main_date_:
                                        count_raiting = count_raiting + 1
                                    raiting.append(select_result[i][3])

                                if count_raiting > 10:
                                    # все оценки в ключевом месяце, обрезаем список до нужной длины
                                    raiting_mod = raiting[size_frame - count_raiting:size_frame]
                                elif size_frame < 10:
                                    # всего оценок < 10, используем все
                                    raiting_mod = raiting
                                else:
                                    # отрежем 10 оценок с конца списка
                                    raiting_mod = raiting[size_frame - 10:size_frame]

                                # вычислим рейтинг тикера
                                ticker_raiting = raiting_calculation(raiting_mod)
                                # print('Рейтинг тикера ', current_ticker, ' = ', ticker_raiting)

                                # теперь обновим рейтинг в сводной таблице
                                # не обновляем, в таблицу пишутся значения recommendation_trend из yahoo_agent_info_2
                                q = 'UPDATE {table} SET Date_r_avto = "{new_value}" WHERE Ticker=?'
                                q_mod = q.format(table=name_main_table, new_value=sql_obj.current_date())
                                # cursorObj.execute(q_mod, (current_ticker,))
                                # con.commit()
                                q = 'UPDATE {table} SET Recom_avto = "{new_value}" WHERE Ticker=?'
                                q_mod = q.format(table=name_main_table, new_value=ticker_raiting)
                                # cursorObj.execute(q_mod, (current_ticker,))
                                # con.commit()
                        else:
                            print("Таблица ", name_table_recommendation, " не существует")
                            logging.warning('Таблица %s не существует', name_table_recommendation)

                    # включаем задержку перед новым запросом на сервер
                    print("Получили информацию по тикеру ", current_ticker)
                    logging.info('Получили информацию по тикеру %s', current_ticker)
                    sleep(frequency_req_serv_info)
        else:
            print("Таблица ", name_main_table, " не существует. Нужно создать ее вручную. Завершение работы")
            logging.warning('Таблица %s не существует. Нужно создать ее вручную', name_main_table)
            sql_obj.write_event_in_system_log(con, cursorObj, sql_obj.current_date(), name_program, comment_prog, 1)
            con.close()
            quit()

    if 1 == 1:
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
                # так же проверяем статус тикера (листинг/делистинг)
                if (row[0] >= index_market) and (row[5] == 'Yes'):
                    # в тикерах индексов Yahoo есть символ ^, который не воспринимается БД
                    # поэтому в качестве тикера для создания таблиц используем колонку 'Ticker_DB'
                    current_ticker = row[1]
                    ticker_name_table = row[6]
                    logging.info('Текущий тикер %s', ticker_name_table)
                    ticker_obj = yf.Ticker(current_ticker + prefix_on_right)
                    # получим информацию
                    ticker_obj.info

                    # если нет таблицы дневных котировок и есть данные,
                    # создадим таблицу и закачаем данные
                    name_table_price_day = ticker_name_table + '_' + '1d'
                    if sql_obj.check_table_is_exists(cursorObj, name_table_price_day):
                        print("Таблица ", name_table_price_day, " существует")
                        logging.info('Таблица %s существует', name_table_price_day)
                        # ничего не делаем, будем ее заполнять на этапе получения часовых данных
                        pass
                    else:
                        # получим котировки за последние 5 лет
                        data = ticker_obj.history(period="5y")
                        if data is None or len(data) == 0:
                            pass
                        else:
                            # print(data)
                            print("Таблица ", name_table_price_day, " не обнаружена")
                            logging.warning('Таблица %s не обнаружена', name_table_price_day)

                            # добавляем новые столбцы
                            data['datetime'] = data.index
                            data['date'] = data['datetime'].dt.date
                            data['time'] = data['datetime'].dt.time

                            # формируем список для загрузки в БД
                            data_load = []
                            size_frame = len(data)
                            for i in range(size_frame):
                                row_market_data = (ticker_name_table,
                                                   '1d',
                                                   datetime.combine(data['date'][i], data['time'][i]),
                                                   round(data['Open'][i], 2),
                                                   round(data['High'][i], 2),
                                                   round(data['Low'][i], 2),
                                                   round(data['Close'][i], 2),
                                                   round(data['Close'][i], 2),
                                                   float(data['Volume'][i]))
                                data_load.append(row_market_data)
                            # print(data_load)
                            if len(data_load) > 1:
                                # создадим таблицу для загрузки котировок
                                sql_obj.create_table_market_data(con, cursorObj, name_table_price_day)
                                # загружать нужно только полные дни. если загрузка происходит в будний день
                                # обрезаем последнюю строку в списке
                                if day_week == 5 or day_week == 6:
                                    sql_obj.insert_many_rows(con, cursorObj, name_table_price_day, data_load)
                                else:
                                    size_list = len(data_load)
                                    del data_load[size_list - 1]
                                    sql_obj.insert_many_rows(con, cursorObj, name_table_price_day, data_load)

                    # если нет таблицы недельных котировок и есть данные,
                    # создадим таблицу и закачаем данные
                    name_table_price_week = ticker_name_table + '_' + '1wk'
                    if sql_obj.check_table_is_exists(cursorObj, name_table_price_week):
                        print("Таблица ", name_table_price_week, " существует")
                        logging.info('Таблица %s существует', name_table_price_week)
                        # ничего не делаем, будем ее заполнять на этапе получения часовых данных
                        pass
                    else:
                        # получим котировки за последние 10 лет
                        data = ticker_obj.history(period="10y", interval='1wk')
                        if data is None or len(data) == 0:
                            pass
                        else:
                            # print(data)
                            print("Таблица ", name_table_price_week, " не обнаружена")
                            logging.warning('Таблица %s не обнаружена', name_table_price_week)

                            # добавляем новые столбцы
                            data['datetime'] = data.index
                            data['date'] = data['datetime'].dt.date
                            data['time'] = data['datetime'].dt.time

                            # формируем список для загрузки в БД
                            data_load = []
                            size_frame = len(data)
                            for i in range(size_frame):
                                # загружать нужно только полные недели
                                # день котировки должен быть понедельник, полная неделя образуется в субботу
                                # поэтому между датой загрузки (текущая дата) и датой котировки должно быть не
                                # менее 6 дней
                                price_date = data['date'][i]
                                price_day = price_date.day
                                price_day_week = price_date.weekday()
                                delta = date.today() - price_date
                                if price_day_week == 0 and delta.total_seconds() >= 6 * 24 * 60 * 60:
                                    if data['Close'][i] is not None and data['Close'][i] > 0:
                                        row_market_data = (ticker_name_table,
                                                           '1wk',
                                                           datetime.combine(data['date'][i], data['time'][i]),
                                                           round(data['Open'][i], 2),
                                                           round(data['High'][i], 2),
                                                           round(data['Low'][i], 2),
                                                           round(data['Close'][i], 2),
                                                           round(data['Close'][i], 2),
                                                           float(data['Volume'][i]))
                                        data_load.append(row_market_data)

                            if len(data_load) > 0:
                                # создадим таблицу для загрузки котировок
                                sql_obj.create_table_market_data(con, cursorObj, name_table_price_week)
                                sql_obj.insert_many_rows(con, cursorObj, name_table_price_week, data_load)

                    # включаем задержку перед новым запросом на сервер
                    print("Получили информацию по тикеру ", ticker_name_table)
                    logging.info('Получили информацию по тикеру %s', ticker_name_table)
                    sleep(frequency_req_serv_info)
        else:
            print("Таблица ", name_table, " не существует. Нужно создать ее вручную. Завершение работы")
            logging.warning('Таблица %s не существует. Нужно создать ее вручную', name_table)
            sql_obj.write_event_in_system_log(con, cursorObj, sql_obj.current_date(), name_program, comment_prog, 1)
            con.close()
            quit()

    sql_obj.write_event_in_system_log(con, cursorObj, sql_obj.current_date(), name_program, comment_prog, 0)
    con.close()

