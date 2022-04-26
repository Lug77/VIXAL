import requests
from bs4 import BeautifulSoup
from portfolio_package import sqlite_modul as sqm
from portfolio_package import analyst_modul as anm
from portfolio_package import send_message_modul as sms
import configparser
import os
import logging
from time import sleep
from datetime import datetime, time
from dateutil.parser import parse
import random

name_program = 'parser_arsagera_potential'
comment_prog = 'получение потенциальной доходности акций для российского рынка с сайта arsagera.ru'

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

# отправка сообщений
post_server_name = 'smtp.yandex.ru'  # отправитель
post_server_port = '465'
email_box_from = "your_email"
password_email_box = "password"
email_box_to = "your_email"  # получатель


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
    global post_server_name
    global post_server_port
    global email_box_from
    global password_email_box
    global email_box_to

    # парсер используется только для базы rus, поэтому сразу выбираем нужный путь из config-файла
    path = config.get("Settings", "path_1")
    name_db = config.get("Settings", "name_db_1")
    name_main_table = config.get("Settings", "name_main_table")
    index_ticker = int(config.get("Settings", "index_ticker"))
    post_server_name = config.get("Settings", "post_server_name")
    post_server_port = config.get("Settings", "post_server_port")
    email_box_from = config.get("Settings", "email_box_from")
    password_email_box = config.get("Settings", "password_email_box")
    email_box_to = config.get("Settings", "email_box_to")


def target_trend_modul(conn_obj, curs_obj, ticker: str, target: float):
    # проверим существование таблицы
    name_table_target = ticker + '_Target_trend' + '_arsagera'
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
        target_0m = target
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


def get_date_update_page(sitemap_url: str, find_url: str):
    url_page = sitemap_url
    r = requests.get(url_page, params=None)
    if r.status_code == 200:
        # получен ответ сервера
        items = BeautifulSoup(r.text, 'html.parser')
        # print(soup)
        date_update = ''
        for item in items.find_all('url'):
            if item.find('loc').get_text() == find_url:
                date_update = item.find('lastmod').get_text()
                break
        date_update_ = parse(date_update)
        date_update_1 = datetime(date_update_.year, date_update_.month, date_update_.day)
        return date_update_1
    else:
        print('Страница ', url_page, ' не отвечает. Ответ сервера ', r.status_code)
        logging.warning('Страница %s не отвечает. Ответ сервера %d', url_page, r.status_code)
        return 0


####################################################################################################
logging.basicConfig(filename='parser_arsagera.log',
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
    control_table_sys_var = sql_obj.check_table_is_exists(cursorObj, '_System_variables')
    if control_table_sys_var is False:
        print('Таблицы _System_variables не существует. Нужно создать ее вручную')
        logging.warning('Таблицы %s не существует. Нужно создать ее вручную', '_System_variables')
    control_table_sys_log = sql_obj.check_table_is_exists(cursorObj, '_System_log')
    if control_table_sys_log is False:
        print('Таблицы _System_log не существует. Нужно создать ее вручную')
        logging.warning('Таблицы %s не существует. Нужно создать ее вручную', '_System_log')
    if control_table_sys_var is False or control_table_sys_log is False:
        con.close()
        quit()

    # получаем дату последнего обновления страницы сайта из системных переменных
    date_update_page = sql_obj.get_system_var_value(cursorObj, 'Date_update_page_arsagera', 'date')
    date_update_page_ = parse(date_update_page)

    # получим дату последнего обновления страницы непосредственно с сайта
    site_map_url = 'https://arsagera.ru/sitemap.xml'
    find_page_url = 'https://arsagera.ru/analitika/emitenty/'
    date_update_from_site = get_date_update_page(site_map_url, find_page_url)
    # print(date_update_page_)
    # print(date_update_from_site)
    # обновим дату проверки в таблице системных переменных
    sql_obj.set_date_control_var(con, cursorObj, sql_obj.current_date(), 'Date_update_page_arsagera', 'date')
    # если сайт не ответил - выход + запись с кодом 1 в системный журнал
    if date_update_from_site == 0:
        sql_obj.write_event_in_system_log(sql_obj.current_date(), name_program, comment_prog, 1)
        con.close()
        quit()

    # если даты равны и таблица _Compare_target существует, значит уже была загрузка этих данных
    # отмечаем успешное завершение программы в системном журнале и выход
    control_table = sql_obj.check_table_is_exists(cursorObj, '_Compare_target')
    if control_table and (date_update_page_ == date_update_from_site):
        sql_obj.write_event_in_system_log(con, cursorObj, sql_obj.current_date(), name_program, comment_prog, 0)
        con.close()
        quit()

    # даты отличаются или нет таблицы _Compare_target - загружаем инфу с сайта
    ticker_list = []
    # ticker_list = ['AFKS', 'AFLT', 'AGRO', 'AKRN', 'ALRS', 'BANE', 'BANEP', 'BELU', 'BSPB', 'CHMF', 'DSKY', 'ENPG', 'ENRU', 'ETLN', 'FEES', 'FIVE', 'FLOT', 'GAZP', 'GCHE', 'GLTR', 'GMKN', 'HYDR', 'IRAO', 'KAZT', 'KRKNP', 'LKOH', 'LSRG', 'MAGN', 'MAIL', 'MDMG', 'MGNT', 'MOEX', 'MRKC', 'MRKP', 'MRKU', 'MRKV', 'MSNG', 'MSRS', 'MTLRP', 'MTSS', 'MVID', 'NKNCP', 'NLMK', 'NMTP', 'NVTK', 'OGKB', 'PHOR', 'PIKK', 'PLZL', 'POGR', 'POLY', 'QIWI', 'RASP', 'ROSN', 'RSTI', 'RSTIP', 'RTKM', 'RTKMP', 'RUAL', 'SBER', 'SIBN', 'SNGS', 'SNGSP', 'TATN', 'TATNP', 'TCSG', 'TGKA', 'TRMK', 'TRNFP', 'TTLK', 'UPRO', 'VSMO', 'VTBR', 'YNDX', 'HHRU', 'LNTA']
    target_list = []
    # target_list = [38.31, -100.0, 44.42, 21.55, 34.43, 42.3, 51.82, 10.81, 59.64, 29.2, 37.77, 56.14, 72.99, 57.09, 32.94, 13.06, 18.96, 44.66, 64.14, 22.2, 32.57, 26.81, 29.1, 22.44, 25.84, 38.88, 85.45, 41.29, 11.74, 19.93, 23.88, 1.78, 46.7, 51.48, 28.66, 49.0, 14.87, 30.55, 0.0, 39.1, 41.75, 72.11, 36.49, 33.02, 41.32, 57.23, 22.0, 8.04, 23.08, 27.48, 35.69, 24.14, 40.32, 50.32, 44.08, 24.04, 27.57, 22.12, 57.11, 45.44, 51.66, 47.0, 41.28, 57.36, 53.15, -3.92, 21.26, 31.23, 25.5, 33.04, 11.59, 0.43, 12.19, -6.1, -7.96, 29.8]
    url_page = 'https://arsagera.ru/analitika/emitenty/'
    r = requests.get(url_page, headers=headers, params=None)
    if r.status_code == 200:
        # получен ответ сервера
        print('Ответ сервера для тикера для страницы ', url_page, ' ', r.status_code)
        logging.info('Ответ сервера для страницы %s %d', url_page, r.status_code)
        soup = BeautifulSoup(r.text, 'html.parser')
        items_tickers_all = soup.find_all('div', class_='tab')

        # группа 6.1 находится в items_tickers[0]
        items_tickers_1 = items_tickers_all[0].find_all('td')
        # группа 6.2 находится в items_tickers[1]
        items_tickers_2 = items_tickers_all[1].find_all('td')
        # группа 6.3 находится в items_tickers[2]
        items_tickers_3 = items_tickers_all[2].find_all('td')
        # группа 6.4 находится в items_tickers[3]
        items_tickers_4 = items_tickers_all[3].find_all('td')

        # items_tickers - список тегов <td>
        # <td>RUAL</td>,
        # <td><a href="https://bf.arsagera.ru/united_company_rusal_plc_rual/" target="_parent" >Русал ОК</a></td>,
        # <td>57,11%</td>

        # если в элементе списка есть тикер, то через один элемент будет потенциал
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

                    # ищем тикер последовательно в каждой из групп
                    items_tickers = items_tickers_1
                    size_frame = len(items_tickers)
                    for i in range(size_frame):
                        s = items_tickers[i].get_text()
                        if current_ticker == s:
                            if i + 2 < size_frame:
                                s_ = items_tickers[i+2].get_text()
                                s_1 = s_.replace('%', ' ')
                                s_2 = s_1.replace(',', '.')
                                potential = float(s_2.strip())
                                ticker_list.append(current_ticker)
                                target_list.append(potential)
                                continue

                    items_tickers = items_tickers_2
                    size_frame = len(items_tickers)
                    for i in range(size_frame):
                        s = items_tickers[i].get_text()
                        if current_ticker == s:
                            if i + 2 < size_frame:
                                s_ = items_tickers[i+2].get_text()
                                s_1 = s_.replace('%', ' ')
                                s_2 = s_1.replace(',', '.')
                                potential = float(s_2.strip())
                                ticker_list.append(current_ticker)
                                target_list.append(potential)
                                continue

                    items_tickers = items_tickers_3
                    size_frame = len(items_tickers)
                    for i in range(size_frame):
                        s = items_tickers[i].get_text()
                        if current_ticker == s:
                            if i + 2 < size_frame:
                                s_ = items_tickers[i+2].get_text()
                                s_1 = s_.replace('%', ' ')
                                s_2 = s_1.replace(',', '.')
                                potential = float(s_2.strip())
                                ticker_list.append(current_ticker)
                                target_list.append(potential)
                                continue

                    items_tickers = items_tickers_4
                    size_frame = len(items_tickers)
                    for i in range(size_frame):
                        s = items_tickers[i].get_text()
                        if current_ticker == s:
                            if i + 2 < size_frame:
                                s_ = items_tickers[i+2].get_text()
                                s_1 = s_.replace('%', ' ')
                                s_2 = s_1.replace(',', '.')
                                potential = float(s_2.strip())
                                ticker_list.append(current_ticker)
                                target_list.append(potential)
                                continue

        # списки должны иметь одинаковую длину
        if len(ticker_list) == len(target_list):
            pass
        else:
            print('Итоговые списки отличаются по длине ', len(ticker_list), ' ', len(target_list))
            logging.warning('Итоговые списки отличаются по длине %d, %d', len(ticker_list),
                            len(target_list))
            sql_obj.write_event_in_system_log(con, cursorObj, sql_obj.current_date(), name_program, comment_prog, 1)
            con.close()
            quit()
    else:
        # страница не отвечает
        print('Ответ сервера для тикера для страницы ', url_page, ' ', r.status_code)
        logging.info('Ответ сервера для страницы %s %d', url_page, r.status_code)
        sql_obj.write_event_in_system_log(con, cursorObj, sql_obj.current_date(), name_program, comment_prog, 1)
        con.close()
        quit()

    # создаем итоговый словарь
    # 'AFKS': 38.31
    result_list = {}
    size_frame = len(ticker_list)
    for i in range(size_frame):
        result_list[ticker_list[i]] = (target_list[i])
    # print(result_list)
    # {'AFKS': 38.31, 'AFLT': -100.0,

    # были извлечены потенциальные доходности в % на дату публикации таблицы
    # теперь нужно вычислить таргет исходя из потенциала
    # date_update_page = datetime(2021, 8, 4)
    date_update_page = date_update_from_site
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
                date_update_page_price = 0
                if current_ticker not in result_list:
                    continue
                potential = result_list[current_ticker]
                current_target = 0
                # проверим наличие таблицы дневных котировок
                name_table_price_day = current_ticker + '_' + '1d'
                if sql_obj.check_table_is_exists(cursorObj, name_table_price_day):
                    print("Таблица ", name_table_price_day, " существует")
                    logging.info('Таблица %s существует', name_table_price_day)

                    # получим цену закрытия тикера на дату обновления страницы
                    time_update_page = time(0, 0, 0)
                    select_date = sql_obj.select_date_time(cursorObj, name_table_price_day,
                                                           date_update_page,
                                                           time_update_page)
                    if select_date is not None:
                        date_update_page_price = select_date[4]

                    # рассчитываем текущий таргет, исходя из потенциала
                    if potential == -100.0:
                        continue
                    else:
                        if date_update_page_price > 0:
                            target = (potential / 100 + 1) * date_update_page_price
                            if target < 1:
                                target = round(target, 5)
                            else:
                                target = round(target, 2)
                            target_trend_modul(con, cursorObj, current_ticker, target)
                        else:
                            continue

                    print("Получили информацию по тикеру ", current_ticker)
                    logging.info('Получили информацию по тикеру %s', current_ticker)
    else:
        print("Таблица ", name_main_table, " не существует")
        logging.warning('Таблица %s не существует', name_main_table)
        sql_obj.write_event_in_system_log(con, cursorObj, sql_obj.current_date(), name_program, comment_prog, 1)
        con.close()
        quit()

    # таргеты, рассчитанные исходя из потенциала загружены
    # запускаем программу для обновления цен в _Compare_target и обновления тикера в _Tickers
    compare_target = anm.AnalystModul(path, name_db, name_main_table, index_ticker)
    compare_target.manipulation_targets()

    # если дата изменилась - обновляем значение системной переменной и отправляем e-mail
    if date_update_page_ != date_update_from_site:
        sql_obj.set_system_var_value(con, cursorObj, date_update_from_site, 'Date_update_page_arsagera', 'date')
        email_obj = sms.SendMessage(email_box_from, password_email_box,
                                    post_server_name, int(post_server_port))
        msg = 'Изменилась дата обновления страницы на сайте Арсагера, новая дата ' + str(date_update_from_site)
        email_obj.send_email(email_box_to, 'Изменилась дата обновления страницы на сайте Арсагера', msg)

    sql_obj.write_event_in_system_log(con, cursorObj, sql_obj.current_date(), name_program, comment_prog, 0)
    con.close()
