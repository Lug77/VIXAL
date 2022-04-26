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

name_program = 'parser_bcs_target'
comment_prog = 'получение таргетов и рекоммендаций для российского рынка с сайта bcs-express.ru'

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


def recommendation_trend_modul(conn_obj, curs_obj, ticker: str, info_tuple: tuple):
    # проверим существование таблицы
    name_table_recommendation = ticker + '_Recommendation_trend'
    if sql_obj.check_table_is_exists(curs_obj, name_table_recommendation):
        print("Таблица ", name_table_recommendation, " существует")
        logging.info('Таблица %s существует', name_table_recommendation)
    else:
        # таблицы не существует. создадим ее
        logging.warning('Таблица %s не обнаружена. Создадим ее', name_table_recommendation)
        q = """ CREATE TABLE {table}
                                    (
                                    Ticker text NOT NULL,
                                    Date_update date DEFAULT '1970.01.01',
                                    Period date DEFAULT '1970.01',
                                    Strong_buy real DEFAULT 0,
                                    Buy real DEFAULT 0,
                                    Hold real DEFAULT 0,
                                    Sell real DEFAULT 0,
                                    Strong_sell real DEFAULT 0,
                                    Sum_recom real DEFAULT 0,
                                    Change real DEFAULT 0,
                                    Change_str text DEFAULT ''
                                    )
                                    """
        q_mod = q.format(table=name_table_recommendation)
        # print(q_mod)
        curs_obj.execute(q_mod)
        conn_obj.commit()

    if sql_obj.check_table_is_exists(curs_obj, name_table_recommendation):
        current_date = datetime.now()
        # истории рекоммендаций нет, есть только текущая рекоммендация с возможными значениями 1, 3 или 5
        # датой рекоммендации будет текущий месяц
        raiting_0m = info_tuple[0]
        raiting_date = datetime(current_date.year, current_date.month, 1, 0, 0, 0)
        r_time = time(0, 0)
        data_load = ()
        if raiting_0m == 1:
            data_load = (ticker,
                         sql_obj.current_date(),
                         raiting_date,
                         1,  # 'strongBuy'
                         0,  # 'buy'
                         0,  # 'hold'
                         0,  # 'sell
                         0,  # 'strongSell'
                         raiting_0m,
                         0,
                         ''
                         )
        elif raiting_0m == 3:
            data_load = (ticker,
                         sql_obj.current_date(),
                         raiting_date,
                         0,  # 'strongBuy'
                         0,  # 'buy'
                         1,  # 'hold'
                         0,  # 'sell
                         0,  # 'strongSell'
                         raiting_0m,
                         0,
                         ''
                         )
        elif raiting_0m == 5:
            data_load = (ticker,
                         sql_obj.current_date(),
                         raiting_date,
                         0,  # 'strongBuy'
                         0,  # 'buy'
                         0,  # 'hold'
                         0,  # 'sell
                         1,  # 'strongSell'
                         raiting_0m,
                         0,
                         ''
                         )
        else:
            data_load = (ticker,
                         sql_obj.current_date(),
                         raiting_date,
                         0,  # 'strongBuy'
                         0,  # 'buy'
                         0,  # 'hold'
                         0,  # 'sell
                         0,  # 'strongSell'
                         raiting_0m,
                         0,
                         ''
                         )
        # если нет строки с соответствующей датой, добавляем ее
        if sql_obj.select_date_time_(curs_obj, name_table_recommendation, 'Period', raiting_date, r_time) is None:
            # print('Строки с указанной датой еще нет')
            q = "INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            q_ = q.format(table=name_table_recommendation)
            curs_obj.execute(q_, data_load)
            conn_obj.commit()
        else:
            # print('Строка с указанной датой уже есть')
            # удаляем старую строку, добавляем новую
            date_condition = datetime.combine(raiting_date, r_time)
            sql_obj.delete_rows_condition(conn_obj, curs_obj, name_table_recommendation, 'Period', date_condition)
            q = "INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            q_ = q.format(table=name_table_recommendation)
            curs_obj.execute(q_, data_load)
            conn_obj.commit()

        # обновляем рейтинг в сводной таблице
        raiting = raiting_0m
        if raiting != 0:
            if sql_obj.check_table_is_exists(curs_obj, name_main_table):
                sql_obj.update_date_in_cell(conn_obj, curs_obj, name_main_table,
                                            'Date_r_avto', raiting_date, 'Ticker',
                                            ticker)
                sql_obj.update_data_in_cell(conn_obj, curs_obj, name_main_table,
                                            'Recom_avto', str(raiting), 'Ticker',
                                            ticker)


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
logging.basicConfig(filename='parser_bcs_target.log',
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
    url_page = 'https://bcs-express.ru/targets'
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
            ticker_list.append(item.get_text())

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

    # ticker_list = ['Okey Gr (LSE)', 'QIWI', 'ROS AGRO (LSE)', 'X5 Group (LSE)', 'Акрон', 'АЛРОСА', 'АФК Система',
    #                'АФК Система (LSE)', 'Аэрофлот', 'Банк Санкт-Петербург', 'Башнефть', 'Башнефть ап', 'ВТБ',
    #                'ВТБ (LSE)', 'Газпнфт (LSE) ', 'Газпром', 'Газпром (LSE)', 'Газпром нефть', 'ГМК Норникель',
    #                'Детский Мир', 'Интер РАО', 'Лента др', 'ЛУКОЙЛ', 'Лукойл (LSE)', 'М.Видео', 'Магнит',
    #                'Магнит (LSE)', 'ММК', 'ММК (LSE)', 'Московская Биржа', 'Мосэнерго', 'МТС', 'МТС (NYSE)', 'НЛМК',
    #                'НЛМК (LSE)', 'НОВАТЭК', 'Новатэк (LSE)', 'НорНик (LSE)', 'ОГК-2', 'Полиметалл', 'Полюс ',
    #                'Роснефть', 'Роснефть(LSE)', 'Россети', 'Ростелеком', 'Ростелеком (LSE)', 'Русагро гдр', 'РусГидро',
    #                'РусГидро (LSE)', 'Сбербанк', 'Сбербанк ап', 'Северсталь', 'Северсталь (LSE)', 'СОЛЛЕРС',
    #                'Сургутнефтегаз', 'Сургутнефтегаз (LSE)', 'Сургутнефтегаз ап', 'Татнефть', 'Татнефть (LSE)',
    #                'Татнефть ап', 'ТГК-1', 'ТМК', 'Транснефть ап', 'ФосАгро', 'ФСК ЕЭС', 'Энел Россия', 'Юнипро',
    #                'Яндекс']
    # recommendation_list = ['Продавать', 'Держать', 'Покупать', 'Покупать', 'Держать', 'Держать', 'Покупать', 'Покупать',
    #                        'Держать', 'Покупать', 'Держать', 'Продавать', 'Покупать', 'Держать', 'Держать', 'Покупать',
    #                        'Покупать', 'Держать', 'Держать', 'Покупать', 'Покупать', 'Держать', 'Покупать', 'Покупать',
    #                        'Покупать', 'Покупать', 'Покупать', 'Покупать', 'Покупать', 'Держать', 'Покупать',
    #                        'Покупать', 'Покупать', 'Покупать', 'Покупать', 'Держать', 'Держать', 'Держать', 'Покупать',
    #                        'Покупать', 'Держать', 'Покупать', 'Покупать', 'Держать', 'Держать', 'Держать', 'Покупать',
    #                        'Покупать', 'Покупать', 'Покупать', 'Покупать', 'Покупать', 'Покупать', 'Держать', 'Держать',
    #                        'Держать', 'Держать', 'Покупать', 'Покупать', 'Держать', 'Покупать', 'Держать', 'Покупать',
    #                        'Держать', 'Покупать', 'Покупать', 'Покупать', 'Покупать']
    # target_list = ['1.0429', '909.9', '17.27', '44.97', '6850.0', '140.20', '46.407', '12.7191', '76.04', '81.17',
    #                '2165.6', '1103.6', '0.05817', '1.4450', '32.39', '373.85', '10.16', '478.33', '27323', '171.92',
    #                '7.8338', '275.85', '7609.6', '103.37', '883.57', '7065.9', '19.2243', '73.599', '13.1117', '189.82',
    #                '2.5048', '397.46', '10.75', '270.79', '37.1083', '1852.54', '251.0938', '37.22', '0.8723',
    #                '1917.77', '16868.2', '690.13', '9.34', '1.4400', '115.49', '9.42', '1260.34', '0.9957', '1.3485',
    #                '397.64', '350.33', '1802.33', '24.5417', '250.0', '64.360', '8.69', '66.411', '665.85', '53.9590',
    #                '618.65', '0.013768', '64.86', '197653', '4434.6', '0.23420', '1.0050', '3.175', '6079.1']

    # если в ticker_list есть префикс LSE, нужно перевести доллары в target_list в рубли
    # нет, непонятно в чем цены на сайте, делаем = 0, чтобы не было 2-х цен
    target_list_ = []
    size_frame = len(ticker_list)
    date_current = datetime.now()
    for i in range(size_frame):
        if ticker_list[i].find('LSE') != -1 or ticker_list[i].find('NYSE') != -1:
            # exchange_rate = sql_obj.get_exchange_rates(date_current.day, date_current.month, date_current.year)
            # target = float(exchange_rate['dollar']) * float(target_list[i])
            # if target < 1:
            #     target = round(target, 5)
            # else:
            #     target = round(target, 2)
            # target_list_.append(target)
            target_list_.append(0)
        else:
            target = float(target_list[i])
            if target < 1:
                target = round(target, 5)
            else:
                target = round(target, 2)
            target_list_.append(target)
    # print(target_list_)

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
    micex_dict = {'Okey Gr (LSE)': 'OKEY', 'QIWI': 'QIWI', 'ROS AGRO (LSE)': 'AGRO', 'X5 Group (LSE)': 'FIVE',
                  'Акрон': 'AKRN', 'АЛРОСА': 'ALRS', 'АФК Система': 'AFKS', 'АФК Система (LSE)': 'AFKS',
                  'Аэрофлот': 'AFLT', 'Банк Санкт-Петербург': 'BSPB', 'Башнефть': 'BANE', 'Башнефть ап': 'BANEP',
                  'ВТБ': 'VTBR', 'ВТБ (LSE)': 'VTBR', 'Газпнфт (LSE) ': 'SIBN', 'Газпром': 'GAZP',
                  'Газпром (LSE)': 'GAZP', 'Газпром нефть': 'SIBN', 'ГМК Норникель': 'GMKN',
                  'Детский Мир': 'DSKY', 'Интер РАО': 'IRAO', 'Лента др': 'LNTA', 'ЛУКОЙЛ': 'LKOH',
                  'Лукойл (LSE)': 'LKOH', 'М.Видео': 'MVID', 'Магнит': 'MGNT', 'Магнит (LSE)': 'MGNT',
                  'ММК': 'MAGN', 'ММК (LSE)': 'MAGN', 'Московская Биржа': 'MOEX', 'Мосэнерго': 'MSNG',
                  'МТС': 'MTSS', 'МТС (NYSE)': 'MTSS', 'НЛМК': 'NLMK', 'НЛМК (LSE)': 'NLMK',
                  'НОВАТЭК': 'NVTK', 'Новатэк (LSE)': 'NVTK', 'НорНик (LSE)': 'GMKN', 'ОГК-2': 'OGKB',
                  'Полиметалл': 'POLY', 'Полюс ': 'PLZL', 'Роснефть': 'ROSN', 'Роснефть(LSE)': 'ROSN',
                  'Россети': 'RSTI', 'Ростелеком': 'RTKM', 'Ростелеком (LSE)': 'RTKM', 'Русагро гдр': 'AGRO',
                  'РусГидро': 'HYDR', 'РусГидро (LSE)': 'HYDR', 'Сбербанк': 'SBER', 'Сбербанк ап': 'SBERP',
                  'Северсталь': 'CHMF', 'Северсталь (LSE)': 'CHMF', 'СОЛЛЕРС': 'SVAV', 'Сургутнефтегаз': 'SNGS',
                  'Сургутнефтегаз (LSE)': 'SNGS', 'Сургутнефтегаз ап': 'SNGSP', 'Татнефть': 'TATN',
                  'Татнефть (LSE)': 'TATN', 'Татнефть ап': 'TATNP', 'ТГК-1': 'TGKA', 'ТМК': 'TRMK',
                  'Транснефть ап': 'TRNFP', 'ФосАгро': 'PHOR', 'ФСК ЕЭС': 'FEES', 'Энел Россия': 'ENRU',
                  'Юнипро': 'UPRO', 'Яндекс': 'YNDX'}

    ticker_list_ = []
    size_frame = len(ticker_list)
    for i in range(size_frame):
        if ticker_list[i] in micex_dict:
            ticker_list_.append(micex_dict[ticker_list[i]])
        else:
            print('Нет тикера ', ticker_list[i], ' в словаре micex_dict')
            logging.warning('Нет тикера %s в словаре micex_dict', ticker_list[i])
            ticker_list_.append('abcd')
    # print(ticker_list_)

    # создаем итоговый словарь. если target = 0 (LSE), в словарь не добавляем
    result_list = {}
    size_frame = len(ticker_list_)
    for i in range(size_frame):
        if target_list_[i] != 0:
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
                    recommendation_trend_modul(con, cursorObj, current_ticker, result_list[current_ticker])
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


