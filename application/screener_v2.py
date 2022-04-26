# обработка информации и построение таблиц и отчетов

import os
from datetime import datetime, date, time
from time import sleep
from portfolio_package import sqlite_modul as sqm
from portfolio_package import send_message_modul as sms
import configparser
import logging
import argparse
from dateutil.parser import parse
from prettytable import PrettyTable
import copy
import telegram
import imgkit
import random
import pandas as pd
import numpy as np

path_config_file = 'settings_expert_system.ini'   # файл с конфигурацией

# аргумент из командной строки для выбора каталога БД
# dir_db = 'RUS'
dir_db = 'USA'

# название БД
path = 'C:\DB_RUS'
name_db = 'rus_market.db'

# имя сводной таблицы
name_main_table = '_Tickers'

# индекс тикера с которого начинать загрузку (=1 - первый тикер в сводной таблице)
index_ticker = 1

# имя таблицы портфеля
name_portfolio_table = ''

# отправка сообщений
post_server_name = 'smtp.yandex.ru'  # отправитель
post_server_port = '465'
email_box_from = "your_email"
password_email_box = "password"
email_box_to = "your_email"  # получатель

# вывод сообщений в консоль
print_comment = False

# параметры запросов
# для БД USA
min_upside = 20                 # потенциал до таргета
max_recommendation = 2.5        # максимальный уровень рекоммендаций аналитиков
min_size_correct = 23           # минимальный уровень коррекции к последнему движению
sector_except = 'Healthcare'    # сектор - исключение
delta_price = 5                 # максимальное отклонение от себестоимости для добора в портфель
upside_for_sell = 10            # потенциал до таргета для уменьшения позиции
recommendation_for_sell = 2.9   # уровень рекоммендаций для уменьшения позиции
# для БД RUS
min_upside_1 = 20
max_recommendation_1 = 3
min_size_correct_1 = 23
sector_except_1 = 'all'         # сектор - исключение
delta_price_1 = 5
upside_for_sell_1 = 10
recommendation_for_sell_1 = 3


# функция для загрузки файла конфигурации
def crud_config(path_, obj_args):
    if not os.path.exists(path_):
        path_system = 'C:\Windows\System32' + '\\' + path_
        if print_comment:
            print("Не обнаружен файл конфигурации в ", path_, ' смотрим в ', path_system)
        logging.warning("Не обнаружен файл конфигурации в %s смотрим в %s", path_, path_system)
        path_ = path_system
        if not os.path.exists(path_):
            if print_comment:
                print("Не обнаружен файл конфигурации и в ", path_)
            logging.warning("Не обнаружен файл конфигурации и в %s", path_)

    config = configparser.ConfigParser()
    config.read(path_)

    # аргумент из командной строки для выбора каталога БД
    global dir_db
    dir_db_ = obj_args.dir_db
    if dir_db_ is None:
        # аргумента в командной строке нет, ничего не делаем
        # используется значение по умолчанию
        pass
    else:
        dir_db = obj_args.dir_db

    # Читаем значения из конфиг. файла и присваиваем их глобальным переменным
    global path
    global name_db
    global name_portfolio_table

    # выбираем путь к БД в зависимости от аргумента в командной строке
    if dir_db == 'usa' or dir_db == 'USA':
        path = config.get("Settings", "path")
        name_db = config.get("Settings", "name_db")
        name_portfolio_table = config.get("Settings", "name_portfolio_table")
        print('Работаем с Базой 1 path ', path, " name_db ", name_db)
        logging.info('Работаем с Базой 1 path %s name_db %s', path, name_db)
    elif dir_db == 'rus' or dir_db == 'RUS':
        path = config.get("Settings", "path_1")
        name_db = config.get("Settings", "name_db_1")
        name_portfolio_table = config.get("Settings", "name_portfolio_table_1")
        print('Работаем с Базой 2 path ', path, " name_db ", name_db)
        logging.info('Работаем с Базой 2 path %s name_db %s', path, name_db)
    else:
        # используются значения по умолчанию
        print('Используются значения по умолчанию path ', path, " name_db ", name_db)
        logging.info('Используются значения по умолчанию path %s name_db %s', path, name_db)
        print('Используются значения по умолчанию name_portfolio_table ', name_portfolio_table)
        logging.info('Используются значения по умолчанию name_portfolio_table %s', name_portfolio_table)

    global name_main_table
    global index_ticker
    global post_server_name
    global post_server_port
    global email_box_from
    global password_email_box
    global email_box_to

    name_main_table = config.get("Settings", "name_main_table")
    index_ticker = int(config.get("Settings", "index_ticker"))
    post_server_name = config.get("Settings", "post_server_name")
    post_server_port = config.get("Settings", "post_server_port")
    email_box_from = config.get("Settings", "email_box_from")
    password_email_box = config.get("Settings", "password_email_box")
    email_box_to = config.get("Settings", "email_box_to")


# расчет годовой дивидендной доходности новый
def calculated_dividends_earning_new(curs_obj, table_name: str, last_close: float):
    q = "SELECT * FROM {table}"
    q_mod = q.format(table=table_name)
    curs_obj.execute(q_mod)
    rows = curs_obj.fetchall()

    # прошлый год
    # количество выплат
    payments_last_year = 0
    # сумма выплат
    summ_last_year = 0
    # размер и дата последней выплаты в прошлом году
    last_payment_last_year = 0
    date_last_payment_last_year = datetime(1970, 1, 1)

    # текущий год
    # количество выплат
    payments_current_year = 0
    # сумма выплат
    summ_current_year = 0
    # размер и дата последней выплаты в текущем году
    last_payment_current_year = 0
    date_last_payment_current_year = datetime(1970, 1, 1)

    current_date = datetime.now()
    current_year = current_date.year
    last_year = current_year - 1

    # годовая дивидендная доходность
    earning_year = 0

    # всего было выплат
    count_payments = 0
    for row in rows:
        # ('АФК Система ао 2004', '2005-05-16 00:00:00', 0.026)
        # print(row)
        # выделяем дату
        date_payment = parse(row[1])
        year_payment = date_payment.year
        if year_payment == last_year:
            # выплаты за прошлый год
            payments_last_year = payments_last_year + 1
            summ_last_year = summ_last_year + row[2]
            if date_payment > date_last_payment_last_year:
                date_last_payment_last_year = date_payment
                last_payment_last_year = row[2]
        if year_payment == current_year:
            # выплаты за текущий год
            payments_current_year = payments_current_year + 1
            summ_current_year = summ_current_year + row[2]
            if date_payment > date_last_payment_current_year:
                date_last_payment_current_year = date_payment
                last_payment_current_year = row[2]
        count_payments = count_payments + 1

    # print('payments_last_year ', payments_last_year)
    # print('summ_last_year ', summ_last_year)
    # print('date_last_payment_last_year ', date_last_payment_last_year)
    # print('last_payment_last_year ', last_payment_last_year)
    #
    # print('payments_current_year ', payments_current_year)
    # print('summ_current_year ', summ_current_year)
    # print('date_last_payment_current_year ', date_last_payment_current_year)
    # print('last_payment_current_year ', last_payment_current_year)

    last_payment = last_payment_current_year
    if last_payment == 0:
        last_payment = last_payment_last_year

    if payments_last_year == 0:
        # дивиденды в прошлом году не выплачивались
        if payments_current_year == 0:
            # в текущем тоже
            pass
        else:
            # в текущем году были выплаты
            earning_year = round((summ_current_year / last_close) * 100, 2)
    else:
        # дивиденды в прошлом году выплачивались payments_last_year раз
        # если выплат в текущем году еще не было, последняя выплата прошлый год * кол-во выплат прошлый гож
        if payments_current_year == 0:
            earning_year = round(((last_payment_last_year * payments_last_year) / last_close) * 100, 2)
        # выплат в текущем году <= выплат в прошлом, последняя выплата текущий год * кол-во выплат прошлый год
        elif (payments_current_year > 0) and (payments_current_year <= payments_last_year):
            earning_year = round(((last_payment_current_year * payments_last_year) / last_close) * 100, 2)
        # выплат в текущем году > выплат в прошлом, берем сумму выплат в текущем году
        elif (payments_current_year > 0) and (payments_current_year > payments_last_year):
            earning_year = round((summ_current_year / last_close) * 100, 2)
        else:
            print('не предусмотренный случай расчета див доходности table_name ', table_name,
                  ' payments_last_year ', payments_last_year,
                  ' payments_current_year ', payments_current_year)
            logging.warning('''не предусмотренный случай расчета див доходности %s
                            payments_last_year %d payments_current_year %d_1''',
                            table_name, payments_last_year, payments_current_year)
    result_list = []
    result_list.append(last_payment)
    result_list.append(count_payments)
    result_list.append(earning_year)
    return result_list


def parsing_line(line: str):
    out_list = []
    # выделяем подстроки по запятым
    sub_line = ""
    size_line = len(line)
    count = 0
    for i in line:
        count = count + 1
        if i == ",":
            out_list.append(sub_line)
            sub_line = ""
        elif i == "\n" or count == size_line:
            sub_line = sub_line + i
            out_list.append(sub_line)
            break
        # пропуск 1-ых пробелов
        elif i == " " and sub_line == "":
            pass
        else:
            sub_line = sub_line + i
    return out_list


# конструирование имени портфеля из заглавных букв
def construct_name_portfolio(line: str):
    name = ''
    size_line = len(line)
    for i in range(size_line):
        if line[i].isupper():
            name = name + line[i]
    return name


# выравнивание строковых данных для поля вывода
def alignment_str_field(line: str, len_field: int):
    if len_field == 0:
        return line
    else:
        # выравнивание строки до len_field
        if len(line) > len_field:
            s_ = line[0:len_field-1]
            return s_
        else:
            s_ = line.ljust(len_field, ' ')
            return s_


# выравнивание числовых данных для поля вывода
def alignment_num_field(value, len_field: int):
    if value != 0:
        if value >= 1:
            s = str(round(value, 2))
            if len_field == 0:
                return s
            else:
                s_ = s.ljust(len_field, ' ')
                return s_
        else:
            s = str(round(value, 4))
            if len_field == 0:
                return s
            else:
                s_ = s.ljust(len_field, ' ')
                return s_
    else:
        if len_field == 0:
            return ''
        else:
            s = ''
            s_ = s.ljust(len_field, ' ')
            return s_


# формирование данных для поля вывода
def get_correct_field(value, len_field: int):
    value_ = ''
    if type(value) == str:
        value_ = alignment_str_field(value, len_field)
    elif type(value) == int or type(value) == float:
        value_ = alignment_num_field(value, len_field)
    else:
        print('не охваченный value ', value, ' type(value) ', type(value))
    return value_


def report_dividends(conn_obj, curs_obj, type_rep: int):
    func_name = 'report_dividends '
    letter_subject = 'Дивидендная доходность БД ' + str(dir_db)     # Тема письма
    len_field = 0                                                   # Минимальная ширина колонки для письма
    name_table_report = '_Report_dividends'                         # Название таблицы отчета в БД
    # определяем количество портфелей для анализа
    name_portfolio_table_ = parsing_line(name_portfolio_table)
    size_list_portfolio = len(name_portfolio_table_)

    # Названия портфелей для колонок отчета формируем из заглавных букв портфелей
    name_portf_1 = ''
    name_portf_2 = ''
    name_portf_3 = ''
    if size_list_portfolio >= 1:
        name_portf_1 = construct_name_portfolio(name_portfolio_table_[0])
    if size_list_portfolio >= 2:
        name_portf_2 = construct_name_portfolio(name_portfolio_table_[1])
    if size_list_portfolio >= 3:
        name_portf_3 = construct_name_portfolio(name_portfolio_table_[2])
    # Названия колонок отчета
    column_0 = 'Ticker'                                     # Тикер
    column_1 = 'Ticker_name'                                # Имя
    column_12 = 'Sector'                                    # Сектор
    column_2 = 'Dividend'                                   # Дивиденд
    column_3 = 'Where_payments'                             # Было выплат
    column_4 = 'Earning_prc'                                # Доходность,%
    column_5 = 'Recommend'                                  # Рекоммендация
    column_6 = name_portf_1 + '_q'                          # Количество Портфель 1
    column_7 = name_portf_1 + '_s'                          # Сумма по текущим Портфель 1
    column_8 = name_portf_2 + '_q'                          # Количество Портфель 2
    column_9 = name_portf_2 + '_s'                          # Сумма по текущим Портфель 2
    column_10 = name_portf_3 + '_q'                         # Количество Портфель 3
    column_11 = name_portf_3 + '_s'                         # Сумма по текущим Портфель 3
    # Тип отчета: 0 - письмо, 1 - отчет в БД, 2 - письмо и отчет в БД
    type_report = type_rep

    list_columns = []
    list_type_columns = []
    # нет ни одного портфеля
    if size_list_portfolio == 0:
        list_columns = [column_0, column_1, column_12, column_2, column_3, column_4, column_5]
        list_type_columns = ['text', 'text', 'text', 'real', 'real', 'real', 'real']
    # 1 и более портфелей
    elif size_list_portfolio == 1:
        list_columns = [column_0, column_1, column_12, column_2, column_3, column_4, column_5, column_6,
                        column_7]
        list_type_columns = ['text', 'text', 'text', 'real', 'real', 'real', 'real', 'real', 'real']
    elif size_list_portfolio == 2:
        list_columns = [column_0, column_1, column_12, column_2, column_3, column_4, column_5, column_6,
                        column_7, column_8, column_9]
        list_type_columns = ['text', 'text', 'text', 'real', 'real', 'real', 'real', 'real', 'real',
                             'real', 'real']
    elif size_list_portfolio == 3:
        list_columns = [column_0, column_1, column_12, column_2, column_3, column_4, column_5, column_6,
                        column_7, column_8, column_9, column_10, column_11]
        list_type_columns = ['text', 'text', 'text', 'real', 'real', 'real', 'real', 'real', 'real',
                             'real', 'real', 'real', 'real']
    else:
        print('Портфелей в списке', size_list_portfolio,
              ' что больше максимального значения 3 для формирования таблицы отчета')
        logging.warning('Портфелей в списке %d что больше максимального значения 3 для формирования таблицы отчета',
                        size_list_portfolio)

    if type_report > 0:
        # удаление старой таблицы
        sql_obj.delete_table(conn_obj, curs_obj, name_table_report)
        # создание новой таблицы БД
        sql_obj.create_table_dividend_report(conn_obj, curs_obj, name_table_report,
                                             list_columns, list_type_columns)
    # формирование отчета
    if sql_obj.check_table_is_exists(curs_obj, name_main_table):
        # print(report_dividends + "Таблица ", name_main_table, " существует")
        logging.info('%s Таблица %s существует', func_name, name_main_table)
        # организуем цикл по всем тикерам из сводной таблицы
        q = "SELECT * FROM {table}"
        q_mod = q.format(table=name_main_table)
        cursorObj.execute(q_mod)
        rows = cursorObj.fetchall()
        # итоговый массив куда будем собирать данные в соответствии с названием колонок
        # и перед выгрузкой отчета будем сортировать по годовой доходности
        # Ticker_ID, Ticker, Ticker_name, Sector, Dividend, Where_payments, Earning_prc, Recommend,
        # Quantity_1, Sum_1, \
        # Quantity_2, Sum_2,
        # Quantity_2, Sum_2,)
        q_array = []
        for row in rows:
            # перебор начинаем с указанного индекса (сраниваем с primary key)
            if row[0] >= index_ticker and row[10] == 'Yes':
                # Tickers
                # (1, 'AFKS', 'АФК Система', 'RU000A0DQZE3', 'MICEX', 'Потребительский сектор', '', '1970.01.01', 0.0,
                # 100.0, 'Yes')
                current_ticker = row[1]
                name_table_price_day = current_ticker + '_' + '1d'
                name_table_dividends = current_ticker + '_Dividends'
                # список строки
                q_row = []
                # отчет формируем для тикеров по которым дивиденды > 0
                if sql_obj.check_table_is_exists(cursorObj, name_table_dividends):
                    # сначала удалим из таблицы нулевые выплаты
                    sql_obj.delete_rows_condition(con, cursorObj, name_table_dividends, 'Dividends', 0)
                    # для расчета доходности понадобится цена закрытия
                    if sql_obj.check_table_is_exists(cursorObj, name_table_price_day):
                        last_close = sql_obj.get_last_value_column(curs_obj, name_table_price_day, "Date",
                                                                   "Close")
                        if last_close is not None:
                            if last_close[1] is not None and last_close[1] != 0:
                                last_close_ = last_close[1]
                                # сделаем расчет: размер последней выплаты, количество выплат,
                                # годовая дивидендная доходность
                                dividend_info = calculated_dividends_earning_new(curs_obj, name_table_dividends,
                                                                                 last_close_)
                                # dividend_info --> [0.31, 18, 1.05]
                                # убираем тикеры с нулевыми выплатами, если их нет в портфелях
                                dividend_ = dividend_info[2]
                                q_row.append(current_ticker)
                                q_row.append(row[2])
                                q_row.append(row[5])
                                q_row.append(dividend_info[0])
                                q_row.append(dividend_info[1])
                                q_row.append(dividend_info[2])
                                q_row.append(row[8])

                                if size_list_portfolio == 0:
                                    pass
                                elif size_list_portfolio == 1:
                                    # один портфель, добавляем кол-во акций и сумму
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[0]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[0],
                                                                                               current_ticker)
                                        # None или (7000.0, 18711.0)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                            if dividend_ == 0:
                                                continue
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                elif size_list_portfolio == 2:
                                    zero_quan_portf_1 = True
                                    zero_quan_portf_2 = True
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[0]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[0],
                                                                                               current_ticker)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                            zero_quan_portf_1 = False
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[1]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[1],
                                                                                               current_ticker)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                            zero_quan_portf_2 = False
                                    if zero_quan_portf_1 is True and zero_quan_portf_2 is True and dividend_ == 0:
                                        continue
                                else:
                                    zero_quan_portf_1 = True
                                    zero_quan_portf_2 = True
                                    zero_quan_portf_3 = True
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[0]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[0],
                                                                                               current_ticker)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                            zero_quan_portf_1 = False
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[1]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[1],
                                                                                               current_ticker)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                            zero_quan_portf_2 = False
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[2]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[2],
                                                                                               current_ticker)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                            zero_quan_portf_3 = False
                                    if zero_quan_portf_1 is True and zero_quan_portf_2 is True and zero_quan_portf_3 is True and dividend_ == 0:
                                        continue
                                # print(q_row)
                                q_array.append(q_row)

        # получился массив q_array, который нужно отсортировать по дивидендной доходности по убыванию
        q_array.sort(key=lambda x: x[5], reverse=True)

        # теперь в нулевой индекс нужно добавить id
        size_frame = len(q_array)
        for i in range(size_frame):
            q_array[i].insert(0, i)

        # получился массив q_array, который нужно отправить на почту нет/и/или загрузить в БД
        if type_report > 0:
            # загрузка в БД
            if size_list_portfolio == 0:
                q = "INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?)"
                q_ = q.format(table=name_table_report)
                curs_obj.executemany(q_, q_array)
                conn_obj.commit()
            elif size_list_portfolio == 1:
                q = "INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                q_ = q.format(table=name_table_report)
                curs_obj.executemany(q_, q_array)
                conn_obj.commit()
            elif size_list_portfolio == 2:
                q = "INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                q_ = q.format(table=name_table_report)
                curs_obj.executemany(q_, q_array)
                conn_obj.commit()
            else:
                q = "INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                q_ = q.format(table=name_table_report)
                curs_obj.executemany(q_, q_array)
                conn_obj.commit()

        if type_report == 0 or type_report == 2:
            # отправка на почту
            # добавим колонку id в заголовок
            list_columns.insert(0, 'id')
            mytable = PrettyTable()
            # заголовок таблицы
            mytable.field_names = list_columns

            # форматирование таблицы перед отправкой по email:
            # заменяем 0 пробелами
            # округляем int и float до 2-х знаков, если больше 1
            # округляем до 4-х, если меньше 0
            q_array_format = copy.deepcopy(q_array)
            for i in range(size_frame):
                size_row = len(q_array[i])
                for j in range(size_row):
                    q_array_format[i][j] = get_correct_field(q_array[i][j], len_field)

            # заполнение таблицы
            for i in range(size_frame):
                mytable.add_row(q_array_format[i])

            # преобразование данных в html
            msg = mytable.get_html_string(attributes={"border": 1, "cellpadding": 4,
                                                      "style": "border-width: 1px; border-collapse: collapse;"})
            if msg:
                # отправка на почту
                email_obj = sms.SendMessage(email_box_from, password_email_box,
                                            post_server_name, int(post_server_port))
                email_obj.send_email_html(email_box_to, letter_subject, '', msg)


def potential_return_to_high_year(conn_obj, curs_obj, type_rep: int):
    func_name = 'potential_return_to_high_year '
    letter_subject = 'Потенциальная доходность до максимума года БД ' + str(dir_db)     # Тема письма
    len_field = 0                                                   # Минимальная ширина колонки для письма
    name_table_report = '_Report_return_high'                       # Название таблицы отчета в БД
    # определяем количество портфелей для анализа
    name_portfolio_table_ = parsing_line(name_portfolio_table)
    size_list_portfolio = len(name_portfolio_table_)
    # Названия портфелей для колонок отчета формируем из заглавных букв портфелей
    name_portf_1 = ''
    name_portf_2 = ''
    name_portf_3 = ''
    if size_list_portfolio >= 1:
        name_portf_1 = construct_name_portfolio(name_portfolio_table_[0])
    if size_list_portfolio >= 2:
        name_portf_2 = construct_name_portfolio(name_portfolio_table_[1])
    if size_list_portfolio >= 3:
        name_portf_3 = construct_name_portfolio(name_portfolio_table_[2])
    # Названия колонок отчета
    column_0 = 'Ticker'                                     # Тикер
    column_1 = 'Ticker_name'                                # Имя
    column_12 = 'Sector'                                    # Сектор
    column_4 = 'Return_prc'                                 # Доходность,%
    column_13 = 'Target'                                    # Целевая цена
    column_14 = 'Upside'                                    # Потенциал
    column_15 = 'Hist_high'                                 # Исторический максимум
    column_5 = 'Recommend'                                  # Рекоммендация
    column_6 = name_portf_1 + '_q'                          # Количество Портфель 1
    column_7 = name_portf_1 + '_s'                          # Сумма по текущим Портфель 1
    column_8 = name_portf_2 + '_q'                          # Количество Портфель 2
    column_9 = name_portf_2 + '_s'                          # Сумма по текущим Портфель 2
    column_10 = name_portf_3 + '_q'                         # Количество Портфель 3
    column_11 = name_portf_3 + '_s'                         # Сумма по текущим Портфель 3
    # Тип отчета: 0 - письмо, 1 - отчет в БД, 2 - письмо и отчет в БД
    type_report = type_rep

    list_columns = []
    list_type_columns = []
    # нет ни одного портфеля
    if size_list_portfolio == 0:
        list_columns = [column_0, column_1, column_12, column_4, column_13, column_14, column_15, column_5]
        list_type_columns = ['text', 'text', 'text', 'real', 'real', 'real', 'real', 'real']
    # 1 и более портфелей
    elif size_list_portfolio == 1:
        list_columns = [column_0, column_1, column_12, column_4, column_13, column_14, column_15, column_5,
                        column_6, column_7]
        list_type_columns = ['text', 'text', 'text', 'real', 'real', 'real', 'real', 'real',
                             'real', 'real']
    elif size_list_portfolio == 2:
        list_columns = [column_0, column_1, column_12, column_4, column_13, column_14, column_15, column_5,
                        column_6, column_7, column_8, column_9]
        list_type_columns = ['text', 'text', 'text', 'real', 'real', 'real', 'real', 'real',
                             'real', 'real', 'real', 'real']
    elif size_list_portfolio == 3:
        list_columns = [column_0, column_1, column_12, column_4, column_13, column_14, column_15, column_5,
                        column_6, column_7, column_8, column_9, column_10, column_11]
        list_type_columns = ['text', 'text', 'text', 'real', 'real', 'real', 'real', 'real',
                             'real', 'real', 'real', 'real', 'real', 'real']
    else:
        print('Портфелей в списке', size_list_portfolio,
              ' что больше максимального значения 3 для формирования таблицы отчета')
        logging.warning('Портфелей в списке %d что больше максимального значения для формирования таблицы отчета',
                        size_list_portfolio)

    if type_report > 0:
        # удаление старой таблицы
        sql_obj.delete_table(conn_obj, curs_obj, name_table_report)
        # создание новой таблицы БД
        sql_obj.create_table_dividend_report(conn_obj, curs_obj, name_table_report,
                                             list_columns, list_type_columns)
    # формирование отчета
    if sql_obj.check_table_is_exists(curs_obj, name_main_table):
        # print(report_dividends + "Таблица ", name_main_table, " существует")
        logging.info('%s Таблица %s существует', func_name, name_main_table)
        # организуем цикл по всем тикерам из сводной таблицы
        q = "SELECT * FROM {table}"
        q_mod = q.format(table=name_main_table)
        cursorObj.execute(q_mod)
        rows = cursorObj.fetchall()
        # итоговый массив куда будем собирать данные в соответствии с названием колонок
        # и перед выгрузкой отчета будем сортировать по годовой доходности
        # Ticker_ID, Ticker, Ticker_name, Sector, Return_prc, Target, Upside, Hist_high, Recommend,
        # Quantity_1, Sum_1, \
        # Quantity_2, Sum_2,
        # Quantity_2, Sum_2,)
        q_array = []
        for row in rows:
            # перебор начинаем с указанного индекса (сраниваем с primary key)
            if row[0] >= index_ticker and row[10] == 'Yes':
                # Tickers
                # (1, 'AFKS', 'АФК Система', 'RU000A0DQZE3', 'MICEX', 'Потребительский сектор', 'Industry',
                # 'Date recomm', Recomm, 100.0, 'Yes', Target)
                current_ticker = row[1]
                name_table_price_day = current_ticker + '_' + '1d'
                name_table_price_week = current_ticker + '_' + '1wk'
                # список строки
                q_row = []

                if sql_obj.check_table_is_exists(cursorObj, name_table_price_day):
                    # для расчета доходности понадобится цена закрытия и максимум за последний год
                    if True:
                        last_close = sql_obj.get_last_value_column(curs_obj, name_table_price_day, "Date",
                                                                   "Close")
                        max_price = sql_obj.get_max_column_value(curs_obj, name_table_price_day, "High",
                                                                 str(31536000))
                        # также понадобится исторический максимум за все время
                        history_high = 0
                        if sql_obj.check_table_is_exists(cursorObj, name_table_price_week):
                            history_high_ = sql_obj.get_max_column_value_no_condition(curs_obj, name_table_price_week,
                                                                                     "High")
                            if history_high_ is not None and len(history_high_) != 0:
                                history_high = history_high_[0]

                        if last_close is not None:
                            if last_close[1] is not None and last_close[1] != 0:
                                last_close_ = last_close[1]
                                max_price_ = max_price[0]
                                # доходность до максимума года
                                return_high = round((1 - last_close_ / max_price_) * 100, 2)
                                # расчет потенциала upside по таргетам
                                upside = 0
                                if row[11] > 0:
                                    upside = round((1 - last_close_ / row[11]) * 100, 2)

                                q_row.append(current_ticker)
                                q_row.append(row[2])
                                q_row.append(row[5])
                                q_row.append(return_high)
                                q_row.append(row[11])
                                q_row.append(upside)
                                q_row.append(history_high)
                                q_row.append(row[8])

                                if size_list_portfolio == 0:
                                    pass
                                elif size_list_portfolio == 1:
                                    # один портфель, добавляем кол-во акций и сумму
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[0]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[0],
                                                                                               current_ticker)
                                        # None или (7000.0, 18711.0)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                elif size_list_portfolio == 2:
                                    zero_quan_portf_1 = True
                                    zero_quan_portf_2 = True
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[0]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[0],
                                                                                               current_ticker)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                            zero_quan_portf_1 = False
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[1]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[1],
                                                                                               current_ticker)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                            zero_quan_portf_2 = False
                                    # if zero_quan_portf_1 is True and zero_quan_portf_2 is True and dividend_ == 0:
                                    #     continue
                                else:
                                    zero_quan_portf_1 = True
                                    zero_quan_portf_2 = True
                                    zero_quan_portf_3 = True
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[0]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[0],
                                                                                               current_ticker)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                            zero_quan_portf_1 = False
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[1]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[1],
                                                                                               current_ticker)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                            zero_quan_portf_2 = False
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[2]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[2],
                                                                                               current_ticker)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                            zero_quan_portf_3 = False
                                    # if zero_quan_portf_1 is True and zero_quan_portf_2 is True and zero_quan_portf_3 is True and dividend_ == 0:
                                    #     continue
                                # print(q_row)
                                q_array.append(q_row)

        # получился массив q_array, который нужно отсортировать по годовой доходности по возрастанию
        # ['AFKS', 'АФК Система', 'Потребительский сектор', 22.58, 0.0, 0.0, 0.0, 0.0, 0.0]
        # q_array.sort(key=lambda x: x[5], reverse=True)
        q_array.sort(key=lambda x: x[3])

        # теперь в нулевой индекс нужно добавить id
        size_frame = len(q_array)
        for i in range(size_frame):
            q_array[i].insert(0, i)

        # получился массив q_array, который нужно отправить на почту нет/и/или загрузить в БД
        if type_report > 0:
            # загрузка в БД
            if size_list_portfolio == 0:
                q = "INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)"
                q_ = q.format(table=name_table_report)
                curs_obj.executemany(q_, q_array)
                conn_obj.commit()
            elif size_list_portfolio == 1:
                q = "INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                q_ = q.format(table=name_table_report)
                curs_obj.executemany(q_, q_array)
                conn_obj.commit()
            elif size_list_portfolio == 2:
                q = "INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                q_ = q.format(table=name_table_report)
                curs_obj.executemany(q_, q_array)
                conn_obj.commit()
            else:
                q = "INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                q_ = q.format(table=name_table_report)
                curs_obj.executemany(q_, q_array)
                conn_obj.commit()

        if type_report == 0 or type_report == 2:
            # отправка на почту
            # добавим колонку id в заголовок
            list_columns.insert(0, 'id')
            mytable = PrettyTable()
            # заголовок таблицы
            mytable.field_names = list_columns

            # форматирование таблицы перед отправкой по email:
            # заменяем 0 пробелами
            # округляем int и float до 2-х знаков, если больше 1
            # округляем до 4-х, если меньше 0
            q_array_format = copy.deepcopy(q_array)
            for i in range(size_frame):
                size_row = len(q_array[i])
                for j in range(size_row):
                    q_array_format[i][j] = get_correct_field(q_array[i][j], len_field)

            # заполнение таблицы
            for i in range(size_frame):
                mytable.add_row(q_array_format[i])

            # преобразование данных в html
            msg = mytable.get_html_string(attributes={"border": 1, "cellpadding": 4,
                                                      "style": "border-width: 1px; border-collapse: collapse;"})
            if msg:
                # отправка на почту
                email_obj = sms.SendMessage(email_box_from, password_email_box,
                                            post_server_name, int(post_server_port))
                email_obj.send_email_html(email_box_to, letter_subject, '', msg)


def potential_return_to_high_year_in_range(conn_obj, curs_obj, type_rep: int, range_low: float, range_high: float):
    func_name = 'potential_return_to_high_year_in_range '
    range_low_str = str(round(range_low, 0))
    range_high_str = str(round(range_high, 0))
    # Тема письма
    letter_subject = 'Потенциальная доходность для БД ' + str(dir_db) + ' от ' + range_low_str + ' до ' + range_high_str
    len_field = 0                                                              # Минимальная ширина колонки для письма
    name_table_report = '_Report_return_' + range_low_str + '_to_' + range_high_str     # Название таблицы отчета в БД
    # определяем количество портфелей для анализа
    name_portfolio_table_ = parsing_line(name_portfolio_table)
    size_list_portfolio = len(name_portfolio_table_)
    # Названия портфелей для колонок отчета формируем из заглавных букв портфелей
    name_portf_1 = ''
    name_portf_2 = ''
    name_portf_3 = ''
    if size_list_portfolio >= 1:
        name_portf_1 = construct_name_portfolio(name_portfolio_table_[0])
    if size_list_portfolio >= 2:
        name_portf_2 = construct_name_portfolio(name_portfolio_table_[1])
    if size_list_portfolio >= 3:
        name_portf_3 = construct_name_portfolio(name_portfolio_table_[2])
    # Названия колонок отчета
    column_0 = 'Ticker'                                     # Тикер
    column_1 = 'Ticker_name'                                # Имя
    column_12 = 'Sector'                                    # Сектор
    column_4 = 'Return_prc'                                 # Доходность,%
    column_13 = 'Target'                                    # Целевая цена
    column_14 = 'Upside'                                    # Потенциал
    column_5 = 'Recommend'                                  # Рекоммендация
    column_6 = name_portf_1 + '_q'                          # Количество Портфель 1
    column_7 = name_portf_1 + '_s'                          # Сумма по текущим Портфель 1
    column_8 = name_portf_2 + '_q'                          # Количество Портфель 2
    column_9 = name_portf_2 + '_s'                          # Сумма по текущим Портфель 2
    column_10 = name_portf_3 + '_q'                         # Количество Портфель 3
    column_11 = name_portf_3 + '_s'                         # Сумма по текущим Портфель 3
    # Тип отчета: 0 - письмо, 1 - отчет в БД, 2 - письмо и отчет в БД
    type_report = type_rep

    list_columns = []
    list_type_columns = []
    # в зависимости от количества портфелей создаем списки для заголовка отчета
    # нет ни одного портфеля
    if size_list_portfolio == 0:
        list_columns = [column_0, column_1, column_12, column_4, column_13, column_14, column_5]
        list_type_columns = ['text', 'text', 'text', 'real', 'real', 'real', 'real']
    # 1 и более портфелей
    elif size_list_portfolio == 1:
        list_columns = [column_0, column_1, column_12, column_4, column_13, column_14, column_5,
                        column_6, column_7]
        list_type_columns = ['text', 'text', 'text', 'real', 'real', 'real', 'real',
                             'real', 'real']
    elif size_list_portfolio == 2:
        list_columns = [column_0, column_1, column_12, column_4, column_13, column_14, column_5,
                        column_6, column_7, column_8, column_9]
        list_type_columns = ['text', 'text', 'text', 'real', 'real', 'real', 'real',
                             'real', 'real', 'real', 'real']
    elif size_list_portfolio == 3:
        list_columns = [column_0, column_1, column_12, column_4, column_13, column_14, column_5,
                        column_6, column_7, column_8, column_9, column_10, column_11]
        list_type_columns = ['text', 'text', 'text', 'real', 'real', 'real', 'real',
                             'real', 'real', 'real', 'real', 'real', 'real']
    else:
        print('Портфелей в списке', size_list_portfolio,
              ' что больше максимального значения 3 для формирования таблицы отчета')
        logging.warning('Портфелей в списке %d что больше максимального значения для формирования таблицы отчета',
                        size_list_portfolio)

    if type_report > 0:
        # удаление старой таблицы
        sql_obj.delete_table(conn_obj, curs_obj, name_table_report)
        # создание новой таблицы БД
        sql_obj.create_table_dividend_report(conn_obj, curs_obj, name_table_report,
                                             list_columns, list_type_columns)
    # формирование отчета
    if sql_obj.check_table_is_exists(curs_obj, name_main_table):
        # print(report_dividends + "Таблица ", name_main_table, " существует")
        logging.info('%s Таблица %s существует', func_name, name_main_table)
        # организуем цикл по всем тикерам из сводной таблицы
        q = "SELECT * FROM {table}"
        q_mod = q.format(table=name_main_table)
        cursorObj.execute(q_mod)
        rows = cursorObj.fetchall()
        # итоговый массив куда будем собирать данные в соответствии с названием колонок
        # и перед выгрузкой отчета будем сортировать по годовой доходности
        # Ticker_ID, Ticker, Ticker_name, Sector, Return_prc, Recommend,
        # Quantity_1, Sum_1, \
        # Quantity_2, Sum_2,
        # Quantity_2, Sum_2,)
        q_array = []
        for row in rows:
            # перебор начинаем с указанного индекса (сраниваем с primary key)
            if row[0] >= index_ticker and row[10] == 'Yes':
                # Tickers
                # (1, 'AFKS', 'АФК Система', 'RU000A0DQZE3', 'MICEX', 'Потребительский сектор', '', '1970.01.01', 0.0,
                # 100.0, 'Yes')
                current_ticker = row[1]
                name_table_price_day = current_ticker + '_' + '1d'
                # список строки
                q_row = []
                # отчет формируем для тикеров у которых потенциал в диапазоне range_low - range_high
                if sql_obj.check_table_is_exists(cursorObj, name_table_price_day):
                    # для расчета доходности понадобится цена закрытия и максимум за последний год
                    if True:
                        last_close = sql_obj.get_last_value_column(curs_obj, name_table_price_day, "Date",
                                                                   "Close")
                        max_price = sql_obj.get_max_column_value(curs_obj, name_table_price_day, "High",
                                                                 str(31536000))
                        if last_close is not None:
                            if last_close[1] is not None and last_close[1] != 0:
                                last_close_ = last_close[1]
                                max_price_ = max_price[0]
                                # доходность до максимума года
                                return_high = round((1 - last_close_ / max_price_) * 100, 2)
                                if return_high < range_low or return_high > range_high:
                                    continue
                                # расчет потенциала upside по таргетам
                                upside = 0
                                if row[11] > 0:
                                    upside = round((1 - last_close_ / row[11]) * 100, 2)

                                q_row.append(current_ticker)
                                q_row.append(row[2])
                                q_row.append(row[5])
                                q_row.append(return_high)
                                q_row.append(row[11])
                                q_row.append(upside)
                                q_row.append(row[8])

                                if size_list_portfolio == 0:
                                    pass
                                elif size_list_portfolio == 1:
                                    # один портфель, добавляем кол-во акций и сумму
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[0]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[0],
                                                                                               current_ticker)
                                        # None или (7000.0, 18711.0)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                elif size_list_portfolio == 2:
                                    zero_quan_portf_1 = True
                                    zero_quan_portf_2 = True
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[0]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[0],
                                                                                               current_ticker)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                            zero_quan_portf_1 = False
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[1]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[1],
                                                                                               current_ticker)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                            zero_quan_portf_2 = False
                                    # if zero_quan_portf_1 is True and zero_quan_portf_2 is True and dividend_ == 0:
                                    #     continue
                                else:
                                    zero_quan_portf_1 = True
                                    zero_quan_portf_2 = True
                                    zero_quan_portf_3 = True
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[0]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[0],
                                                                                               current_ticker)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                            zero_quan_portf_1 = False
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[1]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[1],
                                                                                               current_ticker)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                            zero_quan_portf_2 = False
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[2]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[2],
                                                                                               current_ticker)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                            zero_quan_portf_3 = False
                                    # if zero_quan_portf_1 is True and zero_quan_portf_2 is True and zero_quan_portf_3 is True and dividend_ == 0:
                                    #     continue
                                # print(q_row)
                                q_array.append(q_row)

        # получился массив q_array, который нужно отсортировать по доходности и рекоммендациям, по возрастанию
        # ['AFKS', 'АФК Система', 'Потребительский сектор', 22.58, 0.0, 0.0, 0.0, 0.0, 0.0]
        # q_array.sort(key=lambda x: x[5], reverse=True)
        q_array.sort(key=lambda x: x[3])
        q_array.sort(key=lambda x: x[6])

        # теперь в нулевой индекс нужно добавить id
        size_frame = len(q_array)
        for i in range(size_frame):
            q_array[i].insert(0, i)

        # получился массив q_array, который нужно отправить на почту нет/и/или загрузить в БД
        if type_report > 0:
            # загрузка в БД
            if size_list_portfolio == 0:
                q = "INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?)"
                q_ = q.format(table=name_table_report)
                curs_obj.executemany(q_, q_array)
                conn_obj.commit()
            elif size_list_portfolio == 1:
                q = "INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                q_ = q.format(table=name_table_report)
                curs_obj.executemany(q_, q_array)
                conn_obj.commit()
            elif size_list_portfolio == 2:
                q = "INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                q_ = q.format(table=name_table_report)
                curs_obj.executemany(q_, q_array)
                conn_obj.commit()
            else:
                q = "INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                q_ = q.format(table=name_table_report)
                curs_obj.executemany(q_, q_array)
                conn_obj.commit()

        if type_report == 0 or type_report == 2:
            # отправка на почту
            # добавим колонку id в заголовок
            list_columns.insert(0, 'id')
            mytable = PrettyTable()
            # заголовок таблицы
            mytable.field_names = list_columns

            # форматирование таблицы перед отправкой по email:
            # заменяем 0 пробелами
            # округляем int и float до 2-х знаков, если больше 1
            # округляем до 4-х, если меньше 0
            q_array_format = copy.deepcopy(q_array)
            for i in range(size_frame):
                size_row = len(q_array[i])
                for j in range(size_row):
                    q_array_format[i][j] = get_correct_field(q_array[i][j], len_field)

            # заполнение таблицы
            for i in range(size_frame):
                mytable.add_row(q_array_format[i])

            # преобразование данных в html
            msg = mytable.get_html_string(attributes={"border": 1, "cellpadding": 4,
                                                      "style": "border-width: 1px; border-collapse: collapse;"})
            if msg:
                # отправка на почту
                email_obj = sms.SendMessage(email_box_from, password_email_box,
                                            post_server_name, int(post_server_port))
                email_obj.send_email_html(email_box_to, letter_subject, '', msg)


def report_price_down_correction_level(conn_obj, curs_obj, type_rep: int, start_date: datetime, level_fibo: float,
                                       next_level_fibo: float):
    func_name = 'report_price_down_correction_level '
    level_fibo_str_ = str(round(level_fibo, 1))
    level_fibo_str = level_fibo_str_[0:2]
    # Тема письма
    letter_subject = 'Тикеры ниже уровня коррекции ' + level_fibo_str + ' Fibo для БД ' + str(dir_db)
    len_field = 0                                                               # Минимальная ширина колонки для письма
    name_table_report = '_Report_down_fibo_' + level_fibo_str                  # Название таблицы отчета в БД
    # определяем количество портфелей для анализа
    name_portfolio_table_ = parsing_line(name_portfolio_table)
    size_list_portfolio = len(name_portfolio_table_)
    # Названия портфелей для колонок отчета формируем из заглавных букв портфелей
    name_portf_1 = ''
    name_portf_2 = ''
    name_portf_3 = ''
    if size_list_portfolio >= 1:
        name_portf_1 = construct_name_portfolio(name_portfolio_table_[0])
    if size_list_portfolio >= 2:
        name_portf_2 = construct_name_portfolio(name_portfolio_table_[1])
    if size_list_portfolio >= 3:
        name_portf_3 = construct_name_portfolio(name_portfolio_table_[2])
    # Названия колонок отчета
    column_0 = 'Ticker'                                     # Тикер
    column_1 = 'Ticker_name'                                # Имя
    column_12 = 'Sector'                                    # Сектор
    column_2 = 'Max_year'                                   # Максимум за год
    column_3 = 'Minimum'                                    # Минимум с start_date
    column_4 = 'Fibo_level'                                 # Расчетный уровень Fibo
    column_13 = 'Close'                                     # Текущая цена
    column_14 = 'Target'                                    # Целевая цена
    column_15 = 'Upside'                                    # Потенциал
    column_5 = 'Recommend'                                  # Рекоммендация
    column_6 = name_portf_1 + '_q'                          # Количество Портфель 1
    column_7 = name_portf_1 + '_s'                          # Сумма по текущим Портфель 1
    column_8 = name_portf_2 + '_q'                          # Количество Портфель 2
    column_9 = name_portf_2 + '_s'                          # Сумма по текущим Портфель 2
    column_10 = name_portf_3 + '_q'                         # Количество Портфель 3
    column_11 = name_portf_3 + '_s'                         # Сумма по текущим Портфель 3
    # Тип отчета: 0 - письмо, 1 - отчет в БД, 2 - письмо и отчет в БД
    type_report = type_rep

    list_columns = []
    list_type_columns = []
    # в зависимости от количества портфелей создаем списки для заголовка отчета
    # нет ни одного портфеля
    if size_list_portfolio == 0:
        list_columns = [column_0, column_1, column_12, column_2, column_3, column_4, column_13, column_14, column_15,
                        column_5]
        list_type_columns = ['text', 'text', 'text', 'real', 'real', 'real', 'real', 'real', 'real',
                             'real']
    # 1 и более портфелей
    elif size_list_portfolio == 1:
        list_columns = [column_0, column_1, column_12, column_2, column_3, column_4, column_13, column_14, column_15,
                        column_5,
                        column_6, column_7]
        list_type_columns = ['text', 'text', 'text', 'real', 'real', 'real', 'real', 'real', 'real',
                             'real',
                             'real', 'real']
    elif size_list_portfolio == 2:
        list_columns = [column_0, column_1, column_12, column_2, column_3, column_4, column_13, column_14, column_15,
                        column_5,
                        column_6, column_7, column_8, column_9]
        list_type_columns = ['text', 'text', 'text', 'real', 'real', 'real', 'real', 'real', 'real',
                             'real',
                             'real', 'real', 'real', 'real']
    elif size_list_portfolio == 3:
        list_columns = [column_0, column_1, column_12, column_2, column_3, column_4, column_13, column_14, column_15,
                        column_5,
                        column_6, column_7, column_8, column_9, column_10, column_11]
        list_type_columns = ['text', 'text', 'text', 'real', 'real', 'real', 'real', 'real', 'real',
                             'real',
                             'real', 'real', 'real', 'real', 'real', 'real']
    else:
        print('Портфелей в списке', size_list_portfolio,
              ' что больше максимального значения 3 для формирования таблицы отчета')
        logging.warning('Портфелей в списке %d что больше максимального значения для формирования таблицы отчета',
                        size_list_portfolio)

    if type_report > 0:
        # удаление старой таблицы
        sql_obj.delete_table(conn_obj, curs_obj, name_table_report)
        # создание новой таблицы БД
        sql_obj.create_table_dividend_report(conn_obj, curs_obj, name_table_report,
                                             list_columns, list_type_columns)
    # формирование отчета
    if sql_obj.check_table_is_exists(curs_obj, name_main_table):
        # print(report_dividends + "Таблица ", name_main_table, " существует")
        logging.info('%s Таблица %s существует', func_name, name_main_table)
        # организуем цикл по всем тикерам из сводной таблицы
        q = "SELECT * FROM {table}"
        q_mod = q.format(table=name_main_table)
        cursorObj.execute(q_mod)
        rows = cursorObj.fetchall()
        # итоговый массив куда будем собирать данные в соответствии с названием колонок
        # и перед выгрузкой отчета будем сортировать сначала по алфавиту, затем по рекоммендациям
        # Ticker_ID, Ticker, Ticker_name, Sector, Max_year, Minimum, Fibo_level, Close, Recommend,
        # Quantity_1, Sum_1, \
        # Quantity_2, Sum_2,
        # Quantity_2, Sum_2,)
        q_array = []
        for row in rows:
            # перебор начинаем с указанного индекса (сраниваем с primary key)
            if row[0] >= index_ticker and row[10] == 'Yes':
                # Tickers
                # (1, 'AFKS', 'АФК Система', 'RU000A0DQZE3', 'MICEX', 'Потребительский сектор', '', '1970.01.01', 0.0,
                # 100.0, 'Yes')
                current_ticker = row[1]
                name_table_price_day = current_ticker + '_' + '1d'
                # список строки
                q_row = []
                if sql_obj.check_table_is_exists(cursorObj, name_table_price_day):
                    # для расчета понадобятся цена закрытия, максимум последний год, минимум начиная от start_date
                    if True:
                        last_close = sql_obj.get_last_value_column(curs_obj, name_table_price_day, "Date",
                                                                   "Close")
                        # для расчета минимума получим кол-во секунд от стартовой даты
                        delta = date.today() - start_date
                        delta_sec = delta.total_seconds()
                        # min_price = sql_obj.get_min_column_value(cursorObj, name_table_price_day, "Low",
                        #                                          str(delta_sec))
                        min_price = sql_obj.get_min_column_value_and_date(cursorObj, name_table_price_day, "Low",
                                                                          str(delta_sec))
                        # [(10.354, '2020-03-18 00:00:00')]
                        # максимум должен быть после минимума, поэтому ищем его от даты минимума
                        min_date = parse(min_price[0][1])
                        delta = date.today() - min_date.date()
                        delta_sec = delta.total_seconds()
                        max_price = sql_obj.get_max_column_value(curs_obj, name_table_price_day, "High",
                                                                 str(delta_sec))

                        if min_price is None or max_price is None:
                            continue
                        if last_close is not None:
                            if last_close[1] is not None and last_close[1] != 0:
                                last_close_ = last_close[1]
                                max_price_ = max_price[0]
                                min_price_ = min_price[0][0]
                                # print(current_ticker, ' last_close_ ', last_close_, ' max_price_ ', max_price_,
                                # ' min_price_ ', min_price_)
                                if max_price_ is None or min_price_ is None:
                                    continue
                                else:
                                    # рассчитываем уровень коррекции
                                    fibo_level = max_price_ - (level_fibo / 100) * (max_price_ - min_price_)
                                    # и следующий уровень коррекции
                                    fibo_level_next = max_price_ - (next_level_fibo / 100) * (max_price_ - min_price_)
                                if last_close_ > fibo_level or last_close_ < fibo_level_next:
                                    continue

                                # расчет потенциала upside по таргетам
                                upside = 0
                                if row[11] > 0:
                                    upside = round((1 - last_close_ / row[11]) * 100, 2)
                                q_row.append(current_ticker)
                                q_row.append(row[2])            # Имя
                                q_row.append(row[5])            # Сектор
                                q_row.append(max_price_)
                                q_row.append(min_price_)
                                q_row.append(fibo_level)
                                q_row.append(last_close_)
                                q_row.append(row[11])
                                q_row.append(upside)
                                q_row.append(row[8])            # Рекоммендация

                                if size_list_portfolio == 0:
                                    pass
                                elif size_list_portfolio == 1:
                                    # один портфель, добавляем кол-во акций и сумму
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[0]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[0],
                                                                                               current_ticker)
                                        # None или (7000.0, 18711.0)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                elif size_list_portfolio == 2:
                                    zero_quan_portf_1 = True
                                    zero_quan_portf_2 = True
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[0]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[0],
                                                                                               current_ticker)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                            zero_quan_portf_1 = False
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[1]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[1],
                                                                                               current_ticker)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                            zero_quan_portf_2 = False
                                    # if zero_quan_portf_1 is True and zero_quan_portf_2 is True and dividend_ == 0:
                                    #     continue
                                else:
                                    zero_quan_portf_1 = True
                                    zero_quan_portf_2 = True
                                    zero_quan_portf_3 = True
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[0]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[0],
                                                                                               current_ticker)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                            zero_quan_portf_1 = False
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[1]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[1],
                                                                                               current_ticker)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                            zero_quan_portf_2 = False
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[2]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[2],
                                                                                               current_ticker)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                            zero_quan_portf_3 = False
                                    # if zero_quan_portf_1 is True and zero_quan_portf_2 is True and zero_quan_portf_3 is True and dividend_ == 0:
                                    #     continue
                                # print(q_row)
                                q_array.append(q_row)

        # получился массив q_array, который нужно отсортировать по доходности и рекоммендациям, по возрастанию
        # ['AFKS', 'АФК Система', 'Потребительский сектор', 37.976, 10.354, 31.457207999999998, 29.4, 0.0, 0.0, 0.0, 0.0, 0.0]
        # q_array.sort(key=lambda x: x[5], reverse=True)
        # 1-ая сортировка по алфавиту, 2-ая по рекоммендациям
        q_array.sort(key=lambda x: x[0])
        q_array.sort(key=lambda x: x[9])

        # теперь в нулевой индекс нужно добавить id
        size_frame = len(q_array)
        for i in range(size_frame):
            q_array[i].insert(0, i)

        # получился массив q_array, который нужно отправить на почту нет/и/или загрузить в БД
        if type_report > 0:
            # загрузка в БД
            if size_list_portfolio == 0:
                q = '''INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                q_ = q.format(table=name_table_report)
                curs_obj.executemany(q_, q_array)
                conn_obj.commit()
            elif size_list_portfolio == 1:
                q = '''INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                                  ?, ?)'''
                q_ = q.format(table=name_table_report)
                curs_obj.executemany(q_, q_array)
                conn_obj.commit()
            elif size_list_portfolio == 2:
                q = '''INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                                  ?, ?, ?, ?)'''
                q_ = q.format(table=name_table_report)
                curs_obj.executemany(q_, q_array)
                conn_obj.commit()
            else:
                q = '''INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                                  ?, ?, ?, ?, ?, ?)'''
                q_ = q.format(table=name_table_report)
                curs_obj.executemany(q_, q_array)
                conn_obj.commit()

        if type_report == 0 or type_report == 2:
            # отправка на почту
            # добавим колонку id в заголовок
            list_columns.insert(0, 'id')
            mytable = PrettyTable()
            # заголовок таблицы
            mytable.field_names = list_columns

            # форматирование таблицы перед отправкой по email:
            # заменяем 0 пробелами
            # округляем int и float до 2-х знаков, если больше 1
            # округляем до 4-х, если меньше 0
            q_array_format = copy.deepcopy(q_array)
            for i in range(size_frame):
                size_row = len(q_array[i])
                for j in range(size_row):
                    q_array_format[i][j] = get_correct_field(q_array[i][j], len_field)

            # заполнение таблицы
            for i in range(size_frame):
                mytable.add_row(q_array_format[i])

            # преобразование данных в html
            msg = mytable.get_html_string(attributes={"border": 1, "cellpadding": 4,
                                                      "style": "border-width: 1px; border-collapse: collapse;"})
            if msg:
                # отправка на почту
                email_obj = sms.SendMessage(email_box_from, password_email_box,
                                            post_server_name, int(post_server_port))
                email_obj.send_email_html(email_box_to, letter_subject, '', msg)


def report_size_correction(conn_obj, curs_obj, type_rep: int, start_date: datetime):
    func_name = 'report_size_correction '
    name_table_report = '_Report_down_size_correction'                  # Название таблицы отчета в БД
    # определяем количество портфелей для анализа
    name_portfolio_table_ = parsing_line(name_portfolio_table)
    size_list_portfolio = len(name_portfolio_table_)
    # Названия портфелей для колонок отчета формируем из заглавных букв портфелей
    name_portf_1 = ''
    name_portf_2 = ''
    name_portf_3 = ''
    if size_list_portfolio >= 1:
        name_portf_1 = construct_name_portfolio(name_portfolio_table_[0])
    if size_list_portfolio >= 2:
        name_portf_2 = construct_name_portfolio(name_portfolio_table_[1])
    if size_list_portfolio >= 3:
        name_portf_3 = construct_name_portfolio(name_portfolio_table_[2])
    # Названия колонок отчета
    column_0 = 'Ticker'                                     # Тикер
    column_1 = 'Ticker_name'                                # Имя
    column_12 = 'Sector'                                    # Сектор
    column_2 = 'Max_year'                                   # Максимум за год
    column_3 = 'Minimum'                                    # Минимум с start_date
    column_4 = 'Size_correct'                               # Текущий уровень коррекции к последнему движению
    column_13 = 'Close'                                     # Текущая цена
    column_14 = 'Target'                                    # Целевая цена
    column_15 = 'Upside'                                    # Потенциал
    column_5 = 'Recommend'                                  # Рекоммендация
    column_6 = name_portf_1 + '_q'                          # Количество Портфель 1
    column_7 = name_portf_1 + '_s'                          # Сумма по текущим Портфель 1
    column_8 = name_portf_2 + '_q'                          # Количество Портфель 2
    column_9 = name_portf_2 + '_s'                          # Сумма по текущим Портфель 2
    column_10 = name_portf_3 + '_q'                         # Количество Портфель 3
    column_11 = name_portf_3 + '_s'                         # Сумма по текущим Портфель 3
    # Тип отчета: 0 - письмо, 1 - отчет в БД, 2 - письмо и отчет в БД
    type_report = type_rep

    list_columns = []
    list_type_columns = []
    # в зависимости от количества портфелей создаем списки для заголовка отчета
    # нет ни одного портфеля
    if size_list_portfolio == 0:
        list_columns = [column_0, column_1, column_12, column_2, column_3, column_4, column_13, column_14, column_15,
                        column_5]
        list_type_columns = ['text', 'text', 'text', 'real', 'real', 'real', 'real', 'real', 'real',
                             'real']
    # 1 и более портфелей
    elif size_list_portfolio == 1:
        list_columns = [column_0, column_1, column_12, column_2, column_3, column_4, column_13, column_14, column_15,
                        column_5,
                        column_6, column_7]
        list_type_columns = ['text', 'text', 'text', 'real', 'real', 'real', 'real', 'real', 'real',
                             'real',
                             'real', 'real']
    elif size_list_portfolio == 2:
        list_columns = [column_0, column_1, column_12, column_2, column_3, column_4, column_13, column_14, column_15,
                        column_5,
                        column_6, column_7, column_8, column_9]
        list_type_columns = ['text', 'text', 'text', 'real', 'real', 'real', 'real', 'real', 'real',
                             'real',
                             'real', 'real', 'real', 'real']
    elif size_list_portfolio == 3:
        list_columns = [column_0, column_1, column_12, column_2, column_3, column_4, column_13, column_14, column_15,
                        column_5,
                        column_6, column_7, column_8, column_9, column_10, column_11]
        list_type_columns = ['text', 'text', 'text', 'real', 'real', 'real', 'real', 'real', 'real',
                             'real',
                             'real', 'real', 'real', 'real', 'real', 'real']
    else:
        print('Портфелей в списке', size_list_portfolio,
              ' что больше максимального значения для формирования таблицы отчета')
        logging.warning('Портфелей в списке %d что больше максимального значения 3 для формирования таблицы отчета',
                        size_list_portfolio)

    if type_report > 0:
        # удаление старой таблицы
        sql_obj.delete_table(conn_obj, curs_obj, name_table_report)
        # создание новой таблицы БД
        sql_obj.create_table_dividend_report(conn_obj, curs_obj, name_table_report,
                                             list_columns, list_type_columns)
    # формирование отчета
    if sql_obj.check_table_is_exists(curs_obj, name_main_table):
        # print(report_dividends + "Таблица ", name_main_table, " существует")
        logging.info('%s Таблица %s существует', func_name, name_main_table)
        # организуем цикл по всем тикерам из сводной таблицы
        q = "SELECT * FROM {table}"
        q_mod = q.format(table=name_main_table)
        cursorObj.execute(q_mod)
        rows = cursorObj.fetchall()
        # итоговый массив куда будем собирать данные в соответствии с названием колонок
        # и перед выгрузкой отчета будем сортировать по алфавиту
        # Ticker_ID, Ticker, Ticker_name, Sector, Max_year, Minimum, Size_correct, Close, Recommend,
        # Quantity_1, Sum_1, \
        # Quantity_2, Sum_2,
        # Quantity_2, Sum_2,)
        q_array = []
        for row in rows:
            # перебор начинаем с указанного индекса (сраниваем с primary key)
            if row[0] >= index_ticker and row[10] == 'Yes':
                # Tickers
                # (1, 'AFKS', 'АФК Система', 'RU000A0DQZE3', 'MICEX', 'Потребительский сектор', '', '1970.01.01', 0.0,
                # 100.0, 'Yes')
                current_ticker = row[1]
                name_table_price_day = current_ticker + '_' + '1d'
                # список строки
                q_row = []
                if sql_obj.check_table_is_exists(cursorObj, name_table_price_day):
                    # для расчета понадобятся цена закрытия, максимум последний год, минимум начиная от start_date
                    if True:
                        last_close = sql_obj.get_last_value_column(curs_obj, name_table_price_day, "Date",
                                                                   "Close")
                        # для расчета минимума получим кол-во секунд от стартовой даты
                        delta = date.today() - start_date
                        delta_sec = delta.total_seconds()
                        # min_price = sql_obj.get_min_column_value(cursorObj, name_table_price_day, "Low",
                        #                                          str(delta_sec))
                        min_price = sql_obj.get_min_column_value_and_date(cursorObj, name_table_price_day, "Low",
                                                                          str(delta_sec))
                        # [(10.354, '2020-03-18 00:00:00')]
                        # максимум должен быть после минимума, поэтому ищем его от даты минимума
                        min_date = parse(min_price[0][1])
                        delta = date.today() - min_date.date()
                        delta_sec = delta.total_seconds()
                        max_price = sql_obj.get_max_column_value(curs_obj, name_table_price_day, "High",
                                                                 str(delta_sec))

                        if min_price is None or max_price is None:
                            continue
                        if last_close is not None:
                            if last_close[1] is not None and last_close[1] != 0:
                                last_close_ = last_close[1]
                                max_price_ = max_price[0]
                                min_price_ = min_price[0][0]
                                # print(current_ticker, ' last_close_ ', last_close_, ' max_price_ ', max_price_,
                                # ' min_price_ ', min_price_)
                                size_correction = 0
                                if max_price_ is None or min_price_ is None:
                                    continue
                                else:
                                    # рассчитываем размер текущей коррекции
                                    range_ = (max_price_ - min_price_)
                                    if range_ == 0:
                                        continue
                                    else:
                                        size_correction = round(((max_price_ - last_close_) / range_) * 100, 1)

                                # расчет потенциала upside по таргетам
                                upside = 0
                                if row[11] > 0:
                                    upside = round((1 - last_close_ / row[11]) * 100, 2)
                                q_row.append(current_ticker)
                                q_row.append(row[2])            # Имя
                                q_row.append(row[5])            # Сектор
                                q_row.append(max_price_)
                                q_row.append(min_price_)
                                q_row.append(size_correction)
                                q_row.append(last_close_)
                                q_row.append(row[11])
                                q_row.append(upside)
                                q_row.append(row[8])            # Рекоммендация

                                if size_list_portfolio == 0:
                                    pass
                                elif size_list_portfolio == 1:
                                    # один портфель, добавляем кол-во акций и сумму
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[0]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[0],
                                                                                               current_ticker)
                                        # None или (7000.0, 18711.0)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                elif size_list_portfolio == 2:
                                    zero_quan_portf_1 = True
                                    zero_quan_portf_2 = True
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[0]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[0],
                                                                                               current_ticker)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                            zero_quan_portf_1 = False
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[1]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[1],
                                                                                               current_ticker)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                            zero_quan_portf_2 = False
                                    # if zero_quan_portf_1 is True and zero_quan_portf_2 is True and dividend_ == 0:
                                    #     continue
                                else:
                                    zero_quan_portf_1 = True
                                    zero_quan_portf_2 = True
                                    zero_quan_portf_3 = True
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[0]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[0],
                                                                                               current_ticker)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                            zero_quan_portf_1 = False
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[1]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[1],
                                                                                               current_ticker)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                            zero_quan_portf_2 = False
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[2]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[2],
                                                                                               current_ticker)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                            zero_quan_portf_3 = False
                                    # if zero_quan_portf_1 is True and zero_quan_portf_2 is True and zero_quan_portf_3 is True and dividend_ == 0:
                                    #     continue
                                # print(q_row)
                                q_array.append(q_row)

        # получился массив q_array, который нужно отсортировать по алфавиту
        # ['AFKS', 'АФК Система', 'Потребительский сектор', 37.976, 10.354, 31.457207999999998, 29.4, 0.0, 0.0, 0.0, 0.0, 0.0]
        # q_array.sort(key=lambda x: x[5], reverse=True)
        q_array.sort(key=lambda x: x[0])

        # теперь в нулевой индекс нужно добавить id
        size_frame = len(q_array)
        for i in range(size_frame):
            q_array[i].insert(0, i)

        # получился массив q_array, который нужно загрузить в БД
        if type_report > 0:
            # загрузка в БД
            if size_list_portfolio == 0:
                q = '''INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                q_ = q.format(table=name_table_report)
                curs_obj.executemany(q_, q_array)
                conn_obj.commit()
            elif size_list_portfolio == 1:
                q = '''INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                                  ?, ?)'''
                q_ = q.format(table=name_table_report)
                curs_obj.executemany(q_, q_array)
                conn_obj.commit()
            elif size_list_portfolio == 2:
                q = '''INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                                  ?, ?, ?, ?)'''
                q_ = q.format(table=name_table_report)
                curs_obj.executemany(q_, q_array)
                conn_obj.commit()
            else:
                q = '''INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                                  ?, ?, ?, ?, ?, ?)'''
                q_ = q.format(table=name_table_report)
                curs_obj.executemany(q_, q_array)
                conn_obj.commit()


def report_recommendations(conn_obj, curs_obj, type_rep: int):
    func_name = 'report_recommendations '
    # Тема письма
    letter_subject = 'Список тикеров портфеля с рекомендациями для БД ' + str(dir_db)
    len_field = 0                                                               # Минимальная ширина колонки для письма
    name_table_report = '_Report_recommendation'                                # Название таблицы отчета в БД
    # определяем количество портфелей для анализа
    name_portfolio_table_ = parsing_line(name_portfolio_table)
    size_list_portfolio = len(name_portfolio_table_)
    if size_list_portfolio == 0:
        return
    # Названия портфелей для колонок отчета формируем из заглавных букв портфелей
    name_portf_1 = ''
    name_portf_2 = ''
    name_portf_3 = ''
    if size_list_portfolio >= 1:
        name_portf_1 = construct_name_portfolio(name_portfolio_table_[0])
    if size_list_portfolio >= 2:
        name_portf_2 = construct_name_portfolio(name_portfolio_table_[1])
    if size_list_portfolio >= 3:
        name_portf_3 = construct_name_portfolio(name_portfolio_table_[2])
    # Названия колонок отчета
    column_0 = 'Ticker'                                     # Тикер
    column_1 = 'Ticker_name'                                # Имя
    column_12 = 'Sector'                                    # Сектор
    column_13 = 'Target'                                    # Целевая цена
    column_14 = 'Upside'                                    # Потенциал
    column_5 = 'Recommend'                                  # Рекоммендация
    column_6 = name_portf_1 + '_q'                          # Количество Портфель 1
    column_7 = name_portf_1 + '_s'                          # Сумма по текущим Портфель 1
    column_8 = name_portf_2 + '_q'                          # Количество Портфель 2
    column_9 = name_portf_2 + '_s'                          # Сумма по текущим Портфель 2
    column_10 = name_portf_3 + '_q'                         # Количество Портфель 3
    column_11 = name_portf_3 + '_s'                         # Сумма по текущим Портфель 3
    # Тип отчета: 0 - письмо, 1 - отчет в БД, 2 - письмо и отчет в БД
    type_report = type_rep

    list_columns = []
    list_type_columns = []
    # в зависимости от количества портфелей создаем списки для заголовка отчета
    if size_list_portfolio == 1:
        list_columns = [column_0, column_1, column_12, column_13, column_14, column_5,
                        column_6, column_7]
        list_type_columns = ['text', 'text', 'text', 'real', 'real', 'real',
                             'real', 'real']
    elif size_list_portfolio == 2:
        list_columns = [column_0, column_1, column_12, column_13, column_14, column_5,
                        column_6, column_7, column_8, column_9]
        list_type_columns = ['text', 'text', 'text', 'real', 'real', 'real',
                             'real', 'real', 'real', 'real']
    elif size_list_portfolio == 3:
        list_columns = [column_0, column_1, column_12, column_13, column_14, column_5,
                        column_6, column_7, column_8, column_9, column_10, column_11]
        list_type_columns = ['text', 'text', 'text', 'real', 'real', 'real',
                             'real', 'real', 'real', 'real', 'real', 'real']
    else:
        print('Портфелей в списке', size_list_portfolio,
              ' что больше максимального значения 3 для формирования таблицы отчета')
        logging.warning('Портфелей в списке %d что больше максимального значения для формирования таблицы отчета',
                        size_list_portfolio)

    if type_report > 0:
        # удаление старой таблицы
        sql_obj.delete_table(conn_obj, curs_obj, name_table_report)
        # создание новой таблицы БД
        sql_obj.create_table_dividend_report(conn_obj, curs_obj, name_table_report,
                                             list_columns, list_type_columns)
    # формирование отчета
    if sql_obj.check_table_is_exists(curs_obj, name_main_table):
        # print(report_dividends + "Таблица ", name_main_table, " существует")
        logging.info('%s Таблица %s существует', func_name, name_main_table)
        # организуем цикл по всем тикерам из сводной таблицы
        q = "SELECT * FROM {table}"
        q_mod = q.format(table=name_main_table)
        cursorObj.execute(q_mod)
        rows = cursorObj.fetchall()
        # итоговый массив куда будем собирать данные в соответствии с названием колонок
        # и перед выгрузкой отчета будем сортировать по рекоммендациям
        # Ticker_ID, Ticker, Ticker_name, Sector, Recommend,
        # Quantity_1, Sum_1, \
        # Quantity_2, Sum_2,
        # Quantity_2, Sum_2,)
        q_array = []
        for row in rows:
            # перебор начинаем с указанного индекса (сраниваем с primary key)
            if row[0] >= index_ticker and row[10] == 'Yes':
                # Tickers
                # (1, 'AFKS', 'АФК Система', 'RU000A0DQZE3', 'MICEX', 'Потребительский сектор', 'Industry',
                # '1970.01.01', Recom_avto,
                # 100.0, 'Yes', 'Target')
                current_ticker = row[1]
                name_table_price_day = current_ticker + '_' + '1d'
                # список строки
                q_row = []
                if sql_obj.check_table_is_exists(cursorObj, name_table_price_day):
                    # для расчета понадобятся цена закрытия
                    if True:
                        last_close = sql_obj.get_last_value_column(curs_obj, name_table_price_day, "Date",
                                                                   "Close")
                        if last_close is not None:
                            if last_close[1] is not None and last_close[1] != 0:
                                last_close_ = last_close[1]
                                # расчет потенциала upside по таргетам
                                upside = 0
                                if row[11] > 0:
                                    upside = round((1 - last_close_ / row[11]) * 100, 2)
                                q_row.append(current_ticker)
                                q_row.append(row[2])
                                q_row.append(row[5])
                                q_row.append(row[11])
                                q_row.append(upside)
                                q_row.append(row[8])

                                if size_list_portfolio == 1:
                                    # один портфель, добавляем кол-во акций и сумму
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[0]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[0],
                                                                                               current_ticker)
                                        # None или (7000.0, 18711.0)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                            continue
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                elif size_list_portfolio == 2:
                                    zero_quan_portf_1 = True
                                    zero_quan_portf_2 = True
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[0]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[0],
                                                                                               current_ticker)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                            zero_quan_portf_1 = False
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[1]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[1],
                                                                                               current_ticker)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                            zero_quan_portf_2 = False
                                    if zero_quan_portf_1 is True and zero_quan_portf_2 is True:
                                        continue
                                else:
                                    zero_quan_portf_1 = True
                                    zero_quan_portf_2 = True
                                    zero_quan_portf_3 = True
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[0]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[0],
                                                                                               current_ticker)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                            zero_quan_portf_1 = False
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[1]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[1],
                                                                                               current_ticker)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                            zero_quan_portf_2 = False
                                    if sql_obj.check_table_is_exists(curs_obj, name_portfolio_table_[2]):
                                        quantity_sum_ticker = sql_obj.get_quantity_summ_ticker(cursorObj,
                                                                                               name_portfolio_table_[2],
                                                                                               current_ticker)
                                        if quantity_sum_ticker is None:
                                            q_row.append(0.0)
                                            q_row.append(0.0)
                                        else:
                                            q_row.append(quantity_sum_ticker[0])
                                            q_row.append(quantity_sum_ticker[1])
                                            zero_quan_portf_3 = False
                                    if zero_quan_portf_1 is True and zero_quan_portf_2 is True and zero_quan_portf_3 is True:
                                        continue
                                # print(q_row)
                                q_array.append(q_row)

        # получился массив q_array, который нужно отсортировать по рекоммендациям, по возрастанию
        # ['ALL', 'Allstate Corp (The)', '', 2.8, 2.0, 250.6]
        # q_array.sort(key=lambda x: x[5], reverse=True)
        q_array.sort(key=lambda x: x[5])

        # теперь в нулевой индекс нужно добавить id
        size_frame = len(q_array)
        for i in range(size_frame):
            q_array[i].insert(0, i)

        # получился массив q_array, который нужно отправить на почту нет/и/или загрузить в БД
        if type_report > 0:
            # загрузка в БД
            if size_list_portfolio == 1:
                q = "INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)"
                q_ = q.format(table=name_table_report)
                curs_obj.executemany(q_, q_array)
                conn_obj.commit()
            elif size_list_portfolio == 2:
                q = "INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                q_ = q.format(table=name_table_report)
                curs_obj.executemany(q_, q_array)
                conn_obj.commit()
            else:
                q = "INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                q_ = q.format(table=name_table_report)
                curs_obj.executemany(q_, q_array)
                conn_obj.commit()

        if type_report == 0 or type_report == 2:
            # отправка на почту
            # добавим колонку id в заголовок
            list_columns.insert(0, 'id')
            mytable = PrettyTable()
            # заголовок таблицы
            mytable.field_names = list_columns

            # форматирование таблицы перед отправкой по email:
            # заменяем 0 пробелами
            # округляем int и float до 2-х знаков, если больше 1
            # округляем до 4-х, если меньше 0
            q_array_format = copy.deepcopy(q_array)
            for i in range(size_frame):
                size_row = len(q_array[i])
                for j in range(size_row):
                    q_array_format[i][j] = get_correct_field(q_array[i][j], len_field)

            # заполнение таблицы
            for i in range(size_frame):
                mytable.add_row(q_array_format[i])

            # преобразование данных в html
            msg = mytable.get_html_string(attributes={"border": 1, "cellpadding": 4,
                                                      "style": "border-width: 1px; border-collapse: collapse;"})
            if msg:
                # отправка на почту
                email_obj = sms.SendMessage(email_box_from, password_email_box,
                                            post_server_name, int(post_server_port))
                email_obj.send_email_html(email_box_to, letter_subject, '', msg)


def whats_new_to_buy_usa(curs_obj, upside: float, recommendation: float, sector_none: str, sign_compare_sector: str,
                     direct_potential: str, size_correct: float):

    func_name = 'whats_new_to_buy_rus '
    # получим параметры фильтров
    filters_param = get_filters_param()
    # print('min_cap ', filters_param[0], ' max_cap ', filters_param[1], ' min_pe ', filters_param[2],
    #       ' max_pe ', filters_param[3], ' min_margin ', filters_param[4], ' min_beta ', filters_param[5],
    #       ' max_beta ', filters_param[6])
    min_cap = filters_param[0]
    max_cap = filters_param[1]
    min_pe = filters_param[2]
    max_pe = filters_param[3]
    min_margin = filters_param[4]
    min_beta = filters_param[5]
    max_beta = filters_param[6]

    # получим уровни капитализации
    levels_cap = get_levels_cap(min_cap, max_cap)
    lev_cap_0 = levels_cap[0]
    lev_cap_1 = levels_cap[1]
    lev_cap_2 = levels_cap[2]
    lev_cap_3 = levels_cap[3]
    lev_cap_4 = levels_cap[4]

    # sector_none название сектора, который нужно исключить из запроса
    about_sector = ''
    if sign_compare_sector == 'equals':
        if sector_none == 'all':
            about_sector = '. Все сектора'
            sign_compare_1 = '!='
        else:
            about_sector = '. Только сектор ' + sector_none
            sign_compare_1 = '=='
    else:
        if sector_none == 'all':
            about_sector = '. Все сектора'
            sign_compare_1 = '!='
        else:
            about_sector = '. Все сектора кроме ' + sector_none
            sign_compare_1 = '!='

    # direct_potential = up - потенциал до максимума года > upside; down - ниже
    about_potential = ''
    sign_compare = ''
    about_type_investing = ''
    if direct_potential == 'up':
        about_potential = 'выше'
        sign_compare = '>'
        about_type_investing = ' - инвестидея '
    else:
        about_potential = 'ниже'
        sign_compare = '<'
        about_type_investing = ' на долгий срок '

    # рекоммендации по увеличению позиции формируем индивидуально для каждого портфеля
    name_portfolio_table_ = parsing_line(name_portfolio_table)
    size_list_portfolio = len(name_portfolio_table_)
    for i in range(size_list_portfolio):
        # тема письма
        letter_subject = 'Новые акции в портфель' + about_type_investing + '(потенциал выше '
        letter_subject = letter_subject + str(upside) + ', уровень рекоммендаций ниже '
        letter_subject = letter_subject + str(recommendation) + ', максимум за 52 недели ' + about_potential + ' '
        letter_subject = letter_subject + 'таргета, величина коррекции больше ' + str(size_correct) + ')'
        letter_subject = letter_subject + about_sector + ' для БД ' + str(dir_db)
        len_field = 0  # Минимальная ширина колонки для письма

        # название портфеля формируем из заглавных букв
        name_portf = construct_name_portfolio(name_portfolio_table_[i])
        # названия колонок с количествами
        name_column_q_portf = name_portf + '_q'

        # названия таблиц, которые нужны для формирования запроса
        name_table_1 = '_Report_return_high'
        name_table_2 = '_Report_down_size_correction'

        # проверка существования таблиц
        condition_1 = sql_obj.check_table_is_exists(curs_obj, name_table_1)
        condition_2 = sql_obj.check_table_is_exists(curs_obj, name_table_2)
        if condition_1 and condition_2:
            q = ''' 
                SELECT rrh.Ticker, rrh.Ticker_name, rrh.Sector, rrh.Return_prc, rrh.Target, rrh.Upside, rrh.Recommend, 
                       rdsc.Size_correct, rdsc.Close, t.Market_Cap, t.PE, t.Margin, t.Beta
                FROM "_Report_return_high" rrh                            
                JOIN "_Report_down_size_correction" rdsc 
                JOIN "_Tickers" t
                ON
                rrh.Ticker = rdsc.Ticker AND
                rrh.Ticker = t.Ticker AND
                rrh.Upside > {upside} and 
                rrh.Recommend <= {recommendation} and 
                rrh.Return_prc {sign_compare} rrh.Upside AND 
                rrh.Sector {sign_compare_1} '{sector_none}' AND
                rdsc.Size_correct > {size_correct} AND
                rrh.{name_column_portf_1} = 0 AND
                (t.Market_Cap == '{lev_cap_0}' OR t.Market_Cap == '{lev_cap_1}' OR t.Market_Cap == '{lev_cap_2}' OR
                t.Market_Cap == '{lev_cap_3}' OR t.Market_Cap == '{lev_cap_4}') AND
                (t.PE >= {min_pe} AND t.PE < {max_pe}) AND 
                t.Margin >= {min_margin} AND 
                (t.Beta >= {min_beta} AND t.Beta < {max_beta})
                ORDER BY rrh.Upside desc, rdsc.Size_correct desc   
                '''
            q_ = q.format(upside=upside, recommendation=recommendation, size_correct=size_correct,
                          sector_none=sector_none, sign_compare=sign_compare, sign_compare_1=sign_compare_1,
                          name_column_portf_1=name_column_q_portf,
                          lev_cap_0=lev_cap_0, lev_cap_1=lev_cap_1, lev_cap_2=lev_cap_2, lev_cap_3=lev_cap_3,
                          lev_cap_4=lev_cap_4, min_pe=min_pe, max_pe=max_pe, min_margin=min_margin,
                          min_beta=min_beta, max_beta=max_beta)
            # формирование отчета
            curs_obj.execute(q_)
            rows = cursorObj.fetchall()
            # [('POLY', 'Полиметалл', 'Basic Materials', 36.54, 2019.98, 37.82, 3.0, 70.4, Close), (
            # итоговый массив куда будем собирать данные в соответствии с названием колонок
            # и перед выгрузкой отчета будем сортировать по потенциалу
            # Ticker, Ticker_name, Sector, Return_prc, Target, Upside, Recommend, Size_correct, Close, Cap, PE,
            # Margin, Beta

            q_array = []
            for row in rows:
                # список строки
                q_row = []
                q_row.append(row[0])
                q_row.append(row[1])
                q_row.append(row[2])
                q_row.append(row[3])
                q_row.append(row[4])
                q_row.append(row[5])
                q_row.append(row[6])
                q_row.append(row[7])
                q_row.append(row[8])
                q_row.append(row[9])
                q_row.append(row[10])
                q_row.append(row[11])
                q_row.append(row[12])
                q_array.append(q_row)

            # получился массив q_array, который нужно отсортировать по потенциалу, по возрастанию
            q_array.sort(key=lambda x: x[5], reverse=True)

            # формирование письма и отправка на почту
            list_columns = ['Ticker', 'Ticker_name', 'Sector', 'Return_prc', 'Target', 'Upside', 'Recommend',
                            'Size_correct', 'Price_curr', 'Cap', 'PE', 'Margin', 'Beta']
            mytable = PrettyTable()
            mytable.field_names = list_columns

            # форматирование таблицы перед отправкой по email:
            # заменяем 0 пробелами, округляем int и float до 2-х знаков, если больше 1, округляем до 4-х, если меньше 0
            size_frame = len(q_array)
            q_array_format = copy.deepcopy(q_array)
            for i in range(size_frame):
                size_row = len(q_array[i])
                for j in range(size_row):
                    q_array_format[i][j] = get_correct_field(q_array[i][j], len_field)

            # заполнение таблицы
            for i in range(size_frame):
                mytable.add_row(q_array_format[i])

            # преобразование данных в html
            msg = mytable.get_html_string(attributes={"border": 1, "cellpadding": 4,
                                                      "style": "border-width: 1px; border-collapse: collapse;"})
            if msg:
                # отправка на почту
                email_obj = sms.SendMessage(email_box_from, password_email_box,
                                            post_server_name, int(post_server_port))
                email_obj.send_email_html(email_box_to, letter_subject, '', msg)
        else:
            print('Не найдена одна из таблиц: ', name_table_1, ' или ', name_table_2)
            logging.warning('Не найдена одна из таблиц: %s или %s', name_table_1, name_table_2)


def get_filters_param():
    min_cap = sql_obj.get_system_var_value(cursorObj, 'Min_cap', 'text')
    if min_cap == 0:
        min_cap = 'Large'
    max_cap = sql_obj.get_system_var_value(cursorObj, 'Max_cap', 'text')
    if max_cap == 0:
        max_cap = 'Big'
    min_pe = sql_obj.get_system_var_value(cursorObj, 'Min_PE', 'real')
    if min_pe == 0:
        min_pe = 0
    max_pe = sql_obj.get_system_var_value(cursorObj, 'Max_PE', 'real')
    if max_pe == 0:
        max_pe = 100.0
    min_margin = sql_obj.get_system_var_value(cursorObj, 'Min_Margin', 'real')
    if min_margin == 0:
        min_margin = 0
    min_beta = sql_obj.get_system_var_value(cursorObj, 'Min_Beta', 'real')
    if min_beta == 0:
        min_beta = -1
    max_beta = sql_obj.get_system_var_value(cursorObj, 'Max_Beta', 'real')
    if max_beta == 0:
        max_beta = 1.6
    return [min_cap, max_cap, min_pe, max_pe, min_margin, min_beta, max_beta]


def whats_new_to_buy_rus(curs_obj, upside: float, recommendation: float, sector_none: str, sign_compare_sector: str,
                     direct_potential: str, size_correct: float):

    func_name = 'whats_new_to_buy_rus '

    # sector_none название сектора, который нужно исключить из запроса
    about_sector = ''
    if sign_compare_sector == 'equals':
        if sector_none == 'all':
            about_sector = '. Все сектора'
            sign_compare_1 = '!='
        else:
            about_sector = '. Только сектор ' + sector_none
            sign_compare_1 = '=='
    else:
        if sector_none == 'all':
            about_sector = '. Все сектора'
            sign_compare_1 = '!='
        else:
            about_sector = '. Все сектора кроме ' + sector_none
            sign_compare_1 = '!='

    # direct_potential = up - потенциал до максимума года > upside; down - ниже
    about_potential = ''
    sign_compare = ''
    about_type_investing = ''
    if direct_potential == 'up':
        about_potential = 'выше'
        sign_compare = '>'
        about_type_investing = ' - инвестидея '
    else:
        about_potential = 'ниже'
        sign_compare = '<'
        about_type_investing = ' на долгий срок '

    # рекоммендации по увеличению позиции формируем индивидуально для каждого портфеля
    name_portfolio_table_ = parsing_line(name_portfolio_table)
    size_list_portfolio = len(name_portfolio_table_)
    for i in range(size_list_portfolio):
        # тема письма
        letter_subject = 'Новые акции в портфель' + about_type_investing + '(потенциал выше '
        letter_subject = letter_subject + str(upside) + ', уровень рекоммендаций ниже '
        letter_subject = letter_subject + str(recommendation) + ', максимум за 52 недели ' + about_potential + ' '
        letter_subject = letter_subject + 'таргета, величина коррекции больше ' + str(size_correct) + ')'
        letter_subject = letter_subject + about_sector + ' для БД ' + str(dir_db)
        len_field = 0  # Минимальная ширина колонки для письма

        # название портфеля формируем из заглавных букв
        name_portf = construct_name_portfolio(name_portfolio_table_[i])
        # названия колонок с количествами
        name_column_q_portf = name_portf + '_q'

        # названия таблиц, которые нужны для формирования запроса
        name_table_1 = '_Report_return_high'
        name_table_2 = '_Report_down_size_correction'

        # проверка существования таблиц
        condition_1 = sql_obj.check_table_is_exists(curs_obj, name_table_1)
        condition_2 = sql_obj.check_table_is_exists(curs_obj, name_table_2)
        if condition_1 and condition_2:
            q = ''' 
                SELECT rrh.Ticker, rrh.Ticker_name, rrh.Sector, rrh.Return_prc, rrh.Target, rrh.Upside, rrh.Recommend, 
                       rdsc.Size_correct, rdsc.Close
                FROM "_Report_return_high" rrh                            
                JOIN "_Report_down_size_correction" rdsc 
                ON
                rrh.Ticker = rdsc.Ticker AND
                rrh.Upside > {upside} and 
                rrh.Recommend <= {recommendation} and 
                rrh.Return_prc {sign_compare} rrh.Upside AND 
                rrh.Sector {sign_compare_1} '{sector_none}' AND
                rdsc.Size_correct > {size_correct} AND
                rrh.{name_column_portf_1} = 0
                ORDER BY rrh.Upside desc, rdsc.Size_correct desc   
                '''
            q_ = q.format(upside=upside, recommendation=recommendation, size_correct=size_correct,
                          sector_none=sector_none, sign_compare=sign_compare, sign_compare_1=sign_compare_1,
                          name_column_portf_1=name_column_q_portf)
            # формирование отчета
            curs_obj.execute(q_)
            rows = cursorObj.fetchall()
            # [('POLY', 'Полиметалл', 'Basic Materials', 36.54, 2019.98, 37.82, 3.0, 70.4, Close), (
            # итоговый массив куда будем собирать данные в соответствии с названием колонок
            # и перед выгрузкой отчета будем сортировать по потенциалу
            # Ticker, Ticker_name, Sector, Return_prc, Target, Upside, Recommend, Size_correct, Close

            q_array = []
            for row in rows:
                # список строки
                q_row = []
                q_row.append(row[0])
                q_row.append(row[1])
                q_row.append(row[2])
                q_row.append(row[3])
                q_row.append(row[4])
                q_row.append(row[5])
                q_row.append(row[6])
                q_row.append(row[7])
                q_row.append(row[8])
                q_array.append(q_row)

            # получился массив q_array, который нужно отсортировать по потенциалу, по возрастанию
            q_array.sort(key=lambda x: x[5], reverse=True)

            # формирование письма и отправка на почту
            list_columns = ['Ticker', 'Ticker_name', 'Sector', 'Return_prc', 'Target', 'Upside', 'Recommend',
                            'Size_correct', 'Price_curr']
            mytable = PrettyTable()
            mytable.field_names = list_columns

            # форматирование таблицы перед отправкой по email:
            # заменяем 0 пробелами, округляем int и float до 2-х знаков, если больше 1, округляем до 4-х, если меньше 0
            size_frame = len(q_array)
            q_array_format = copy.deepcopy(q_array)
            for i in range(size_frame):
                size_row = len(q_array[i])
                for j in range(size_row):
                    q_array_format[i][j] = get_correct_field(q_array[i][j], len_field)

            # заполнение таблицы
            for i in range(size_frame):
                mytable.add_row(q_array_format[i])

            # преобразование данных в html
            msg = mytable.get_html_string(attributes={"border": 1, "cellpadding": 4,
                                                      "style": "border-width: 1px; border-collapse: collapse;"})
            if msg:
                # отправка на почту
                email_obj = sms.SendMessage(email_box_from, password_email_box,
                                            post_server_name, int(post_server_port))
                email_obj.send_email_html(email_box_to, letter_subject, '', msg)
        else:
            print('Не найдена одна из таблиц: ', name_table_1, ' или ', name_table_2)
            logging.warning('Не найдена одна из таблиц: %s или %s', name_table_1, name_table_2)


def get_filters_param_increase_position_usa():
    min_cap = sql_obj.get_system_var_value(cursorObj, 'Min_cap_USA_incr', 'text')
    if min_cap == 0:
        min_cap = 'Large'
    min_pe = sql_obj.get_system_var_value(cursorObj, 'Min_PE_USA_incr', 'real')
    if min_pe == 0:
        min_pe = 0
    max_pe = sql_obj.get_system_var_value(cursorObj, 'Max_PE_USA_incr', 'real')
    if max_pe == 0:
        max_pe = 100.0
    min_margin = sql_obj.get_system_var_value(cursorObj, 'Min_Margin_USA_incr', 'real')
    if min_margin == 0:
        min_margin = 0
    max_size_grid = sql_obj.get_system_var_value(cursorObj, 'Max_size_grid_USA_incr', 'real')
    if max_size_grid == 0:
        max_size_grid = 1000
    return [min_cap, min_pe, max_pe, min_margin, max_size_grid]


def get_levels_cap(min_cap, max_cap):
    list_cap = ['Micro', 'Small', 'Middle', 'Large', 'Big']
    result_array = ['1', '1', '1', '1', '1']
    flag = False
    flag_1 = True
    for i in range(len(list_cap)):
        if list_cap[i] == max_cap:
            result_array[i] = list_cap[i]
            flag_1 = False
        if ((list_cap[i] == min_cap) or flag) and flag_1:
            result_array[i] = list_cap[i]
            flag = True
    return result_array


def increase_position_in_portfolio_usa(curs_obj, upside: float, recommendation: float, sector_none: str,
                                   sign_compare_sector: str, size_correct: float, delta_price: float):

    func_name = 'increase_position_in_portfolio '

    # получим параметры фильтров
    filters_param = get_filters_param_increase_position_usa()
    min_cap = filters_param[0]
    min_pe = filters_param[1]
    max_pe = filters_param[2]
    min_margin = filters_param[3]
    max_size_grid = filters_param[4]

    # получим уровни капитализации
    levels_cap = get_levels_cap(min_cap, 'Big')
    lev_cap_0 = levels_cap[0]
    lev_cap_1 = levels_cap[1]
    lev_cap_2 = levels_cap[2]
    lev_cap_3 = levels_cap[3]
    lev_cap_4 = levels_cap[4]

    # sector_none название сектора, который нужно исключить из запроса
    about_sector = ''
    if sign_compare_sector == 'equals':
        if sector_none == 'all':
            about_sector = '. Все сектора'
            sign_compare_1 = '!='
        else:
            about_sector = '. Только сектор ' + sector_none
            sign_compare_1 = '=='
    else:
        if sector_none == 'all':
            about_sector = '. Все сектора'
            sign_compare_1 = '!='
        else:
            about_sector = '. Все сектора кроме ' + sector_none
            sign_compare_1 = '!='

    # рекоммендации по увеличению позиции формируем индивидуально для каждого портфеля
    name_portfolio_table_ = parsing_line(name_portfolio_table)
    size_list_portfolio = len(name_portfolio_table_)
    for i in range(size_list_portfolio):
        # тема письма
        letter_subject = 'Увеличить позиции в портфеле ' + name_portfolio_table_[i] + ' с потенциалом выше '
        letter_subject = letter_subject + str(upside) + ', уровнем рекоммендаций ниже '
        letter_subject = letter_subject + str(recommendation)
        letter_subject = letter_subject + ' величиной коррекции больше ' + str(size_correct)
        letter_subject = letter_subject + ' и уровнем цены не более чем на ' + str(delta_price)
        letter_subject = letter_subject + ' % выше себестоимости' + about_sector + ' для БД ' + str(dir_db)
        len_field = 0  # Минимальная ширина колонки для письма

        # название портфеля формируем из заглавных букв
        name_portf = construct_name_portfolio(name_portfolio_table_[i])
        # названия колонок с количествами
        name_column_q_portf = name_portf + '_q'

        # названия таблиц, которые нужны для формирования запроса
        name_table_1 = '_Report_return_high'
        name_table_2 = '_Report_down_size_correction'
        name_table_portf = name_portfolio_table_[i]

        # проверка существования таблиц
        condition_1 = sql_obj.check_table_is_exists(curs_obj, name_table_1)
        condition_2 = sql_obj.check_table_is_exists(curs_obj, name_table_2)
        condition_3 = sql_obj.check_table_is_exists(curs_obj, name_table_portf)
        if condition_1 and condition_2 and condition_3:
            # 1-ый запрос кол-во в портфеле > 1 + фильтры
            q = ''' 
                SELECT rrh.Ticker, rrh.Ticker_name, rrh.Sector, rrh.Return_prc, rrh.Target, rrh.Upside, rrh.Recommend, 
                       rdsc.Size_correct, psl.Quantity_portf, psl.Price_aver, psl.Price_curr, 
	                   psl.Quantity_portf * psl.Price_aver as Summ_portf, psl.Size_grid,
	                   t.Market_Cap, t.PE, t.Margin, t.Beta 
                FROM "_Report_return_high" rrh
                JOIN "{name_portfolio}" psl 
                ON
                rrh.Ticker = psl.Ticker AND 
                rrh.Sector {sign_compare_1} '{sector_none}' AND
                rrh.Upside > {upside} and 
                rrh.Recommend <= {recommendation} and 
                psl.Quantity_portf > 1 AND
                psl.Price_curr / psl.Price_aver < {delta_price} AND
                psl.Size_grid <= {max_size_grid}
                JOIN "_Report_down_size_correction" rdsc 
                ON
                rrh.Ticker = rdsc.Ticker AND 
                rdsc.Size_correct > {size_correct}
                JOIN "_Tickers" t
                ON
                rrh.Ticker = t.Ticker AND
                (t.Market_Cap == '{lev_cap_0}' OR t.Market_Cap == '{lev_cap_1}' OR t.Market_Cap == '{lev_cap_2}' OR
                t.Market_Cap == '{lev_cap_3}' OR t.Market_Cap == '{lev_cap_4}') AND
                (t.PE >= {min_pe} AND t.PE < {max_pe}) AND 
                t.Margin >= {min_margin}
                ORDER BY rrh.Upside desc    
                '''
            q_ = q.format(upside=upside, recommendation=recommendation, size_correct=size_correct,
                          sector_none=sector_none, sign_compare_1=sign_compare_1,
                          name_portfolio=name_portfolio_table_[i],
                          delta_price=str(1 + delta_price / 100), max_size_grid=max_size_grid,
                          lev_cap_0=lev_cap_0, lev_cap_1=lev_cap_1, lev_cap_2=lev_cap_2, lev_cap_3=lev_cap_3,
                          lev_cap_4=lev_cap_4, min_pe=min_pe, max_pe=max_pe, min_margin=min_margin)
            # формирование отчета
            curs_obj.execute(q_)
            rows = cursorObj.fetchall()
            # [('ETLN', 'Группа Эталон ГДР', 'Real Estate', 18.26, 191.15, 40.22, 0.0, 38.7, 185.0, 120.28, 114.0, 21090.0)
            # Ticker, Ticker_name, Sector, Return_prc, Target, Upside, Recommend, Size_correct,
            # Quantity_portf, Price_aver, Price_curr, Summ_portf, Size_grid, Market_Cap, PE, Margin, Beta

            q_array = []
            for row in rows:
                # список строки
                q_row = []
                q_row.append(row[0])
                q_row.append(row[1])
                q_row.append(row[2])
                q_row.append(row[3])
                q_row.append(row[4])
                q_row.append(row[5])
                q_row.append(row[6])
                q_row.append(row[7])
                q_row.append(row[8])
                q_row.append(row[9])
                q_row.append(row[10])
                q_row.append(row[11])
                q_row.append(row[12])
                q_row.append(row[13])
                q_row.append(row[14])
                q_row.append(row[15])
                q_row.append(row[16])
                q_array.append(q_row)

            # получился массив q_array, который нужно отсортировать по потенциалу, по возрастанию
            q_array.sort(key=lambda x: x[5], reverse=True)

            # 2-ой запрос кол-во в портфеле = 1 без фильтров
            q = ''' 
                        SELECT rrh.Ticker, rrh.Ticker_name, rrh.Sector, rrh.Return_prc, rrh.Target, rrh.Upside, rrh.Recommend, 
                               rdsc.Size_correct, psl.Quantity_portf, psl.Price_aver, psl.Price_curr, 
                               psl.Quantity_portf * psl.Price_aver as Summ_portf, psl.Size_grid,
                               t.Market_Cap, t.PE, t.Margin, t.Beta 
                        FROM "_Report_return_high" rrh
                        JOIN "{name_portfolio}" psl 
                        ON
                        rrh.Ticker = psl.Ticker AND 
                        rrh.Sector {sign_compare_1} '{sector_none}' AND
                        rrh.Upside > {upside} and 
                        rrh.Recommend <= {recommendation} and 
                        psl.Quantity_portf = 1 AND
                        psl.Price_curr / psl.Price_aver < {delta_price} 
                        JOIN "_Report_down_size_correction" rdsc 
                        ON
                        rrh.Ticker = rdsc.Ticker AND 
                        rdsc.Size_correct > {size_correct}
                        JOIN "_Tickers" t
                        ON
                        rrh.Ticker = t.Ticker
                        ORDER BY rrh.Upside desc    
                        '''
            q_ = q.format(upside=upside, recommendation=recommendation, size_correct=size_correct,
                          sector_none=sector_none, sign_compare_1=sign_compare_1,
                          name_portfolio=name_portfolio_table_[i],
                          delta_price=str(1 + delta_price / 100))
            # формирование отчета
            curs_obj.execute(q_)
            rows = cursorObj.fetchall()

            # строка разделитель
            q_row = ['----', '----', '----', 0, 0, 0, 0, 0, 0, 0, 0 , 0 , 0, '----', 0, 0, 0]
            q_array.append(q_row)

            for row in rows:
                # список строки
                q_row = []
                q_row.append(row[0])
                q_row.append(row[1])
                q_row.append(row[2])
                q_row.append(row[3])
                q_row.append(row[4])
                q_row.append(row[5])
                q_row.append(row[6])
                q_row.append(row[7])
                q_row.append(row[8])
                q_row.append(row[9])
                q_row.append(row[10])
                q_row.append(row[11])
                q_row.append(row[12])
                q_row.append(row[13])
                q_row.append(row[14])
                q_row.append(row[15])
                q_row.append(row[16])
                q_array.append(q_row)

            # формирование письма и отправка на почту
            list_columns = ['Ticker', 'Ticker_name', 'Sector', 'Return_prc', 'Target', 'Upside', 'Recom',
                            'Size_corr', 'Q_portf', 'Pr_aver', 'Pr_curr', 'Summ_portf', 'Size_grid',
                            'Market_Cap', 'PE', 'Margin', 'Beta']
            mytable = PrettyTable()
            mytable.field_names = list_columns

            # форматирование таблицы перед отправкой по email:
            # заменяем 0 пробелами, округляем int и float до 2-х знаков, если больше 1, округляем до 4-х, если меньше 0
            size_frame = len(q_array)
            q_array_format = copy.deepcopy(q_array)
            for i in range(size_frame):
                size_row = len(q_array[i])
                for j in range(size_row):
                    q_array_format[i][j] = get_correct_field(q_array[i][j], len_field)

            # заполнение таблицы
            for i in range(size_frame):
                mytable.add_row(q_array_format[i])

            # преобразование данных в html
            msg = mytable.get_html_string(attributes={"border": 1, "cellpadding": 4,
                                                      "style": "border-width: 1px; border-collapse: collapse;"})
            if msg:
                # отправка на почту
                email_obj = sms.SendMessage(email_box_from, password_email_box,
                                            post_server_name, int(post_server_port))
                email_obj.send_email_html(email_box_to, letter_subject, '', msg)
        else:
            print('Не найдена одна из таблиц: ', name_table_1, ' или ', name_table_2, ' или ', name_table_portf)
            logging.warning('Не найдена одна из таблиц: %s или %s или %s', name_table_1, name_table_2, name_table_portf)


def increase_position_in_portfolio_rus(curs_obj, upside: float, recommendation: float, sector_none: str,
                                   sign_compare_sector: str, size_correct: float, delta_price: float):

    func_name = 'increase_position_in_portfolio '

    # sector_none название сектора, который нужно исключить из запроса
    about_sector = ''
    if sign_compare_sector == 'equals':
        if sector_none == 'all':
            about_sector = '. Все сектора'
            sign_compare_1 = '!='
        else:
            about_sector = '. Только сектор ' + sector_none
            sign_compare_1 = '=='
    else:
        if sector_none == 'all':
            about_sector = '. Все сектора'
            sign_compare_1 = '!='
        else:
            about_sector = '. Все сектора кроме ' + sector_none
            sign_compare_1 = '!='

    # рекоммендации по увеличению позиции формируем индивидуально для каждого портфеля
    name_portfolio_table_ = parsing_line(name_portfolio_table)
    size_list_portfolio = len(name_portfolio_table_)
    for i in range(size_list_portfolio):
        # тема письма
        letter_subject = 'Увеличить позиции в портфеле ' + name_portfolio_table_[i] + ' с потенциалом выше '
        letter_subject = letter_subject + str(upside) + ', уровнем рекоммендаций ниже '
        letter_subject = letter_subject + str(recommendation)
        letter_subject = letter_subject + ' величиной коррекции больше ' + str(size_correct)
        letter_subject = letter_subject + ' и уровнем цены не более чем на ' + str(delta_price)
        letter_subject = letter_subject + ' % выше себестоимости' + about_sector + ' для БД ' + str(dir_db)
        len_field = 0  # Минимальная ширина колонки для письма

        # название портфеля формируем из заглавных букв
        name_portf = construct_name_portfolio(name_portfolio_table_[i])
        # названия колонок с количествами
        name_column_q_portf = name_portf + '_q'

        # названия таблиц, которые нужны для формирования запроса
        name_table_1 = '_Report_return_high'
        name_table_2 = '_Report_down_size_correction'
        name_table_portf = name_portfolio_table_[i]

        # проверка существования таблиц
        condition_1 = sql_obj.check_table_is_exists(curs_obj, name_table_1)
        condition_2 = sql_obj.check_table_is_exists(curs_obj, name_table_2)
        condition_3 = sql_obj.check_table_is_exists(curs_obj, name_table_portf)
        if condition_1 and condition_2 and condition_3:
            q = ''' 
                SELECT rrh.Ticker, rrh.Ticker_name, rrh.Sector, rrh.Return_prc, rrh.Target, rrh.Upside, rrh.Recommend, 
                       rdsc.Size_correct, psl.Quantity_portf, psl.Price_aver, psl.Price_curr, 
	                   psl.Quantity_portf * psl.Price_aver as Summ_portf, psl.Size_grid 
                FROM "_Report_return_high" rrh
                JOIN "{name_portfolio}" psl 
                ON
                rrh.Ticker = psl.Ticker AND 
                rrh.Sector {sign_compare_1} '{sector_none}' AND
                rrh.Upside > {upside} and 
                rrh.Recommend <= {recommendation} and 
                psl.Quantity_portf > 0 AND
                psl.Price_curr / psl.Price_aver < {delta_price}
                JOIN "_Report_down_size_correction" rdsc 
                ON
                rrh.Ticker = rdsc.Ticker AND 
                rdsc.Size_correct > {size_correct}
                ORDER BY rrh.Upside desc    
                '''
            q_ = q.format(upside=upside, recommendation=recommendation, size_correct=size_correct,
                          sector_none=sector_none, sign_compare_1=sign_compare_1,
                          name_portfolio=name_portfolio_table_[i],
                          delta_price=str(1 + delta_price / 100))
            # формирование отчета
            curs_obj.execute(q_)
            rows = cursorObj.fetchall()
            # [('ETLN', 'Группа Эталон ГДР', 'Real Estate', 18.26, 191.15, 40.22, 0.0, 38.7, 185.0, 120.28, 114.0, 21090.0)
            # Ticker, Ticker_name, Sector, Return_prc, Target, Upside, Recommend, Size_correct,
            # Quantity_portf, Price_aver, Price_curr, Summ_portf, Size_grid

            q_array = []
            for row in rows:
                # список строки
                q_row = []
                q_row.append(row[0])
                q_row.append(row[1])
                q_row.append(row[2])
                q_row.append(row[3])
                q_row.append(row[4])
                q_row.append(row[5])
                q_row.append(row[6])
                q_row.append(row[7])
                q_row.append(row[8])
                q_row.append(row[9])
                q_row.append(row[10])
                q_row.append(row[11])
                q_row.append(row[12])
                q_array.append(q_row)

            # получился массив q_array, который нужно отсортировать по потенциалу, по возрастанию
            q_array.sort(key=lambda x: x[5], reverse=True)

            # формирование письма и отправка на почту
            list_columns = ['Ticker', 'Ticker_name', 'Sector', 'Return_prc', 'Target', 'Upside', 'Recom',
                            'Size_corr', 'Q_portf', 'Pr_aver', 'Pr_curr', 'Summ_portf', 'Size_grid']
            mytable = PrettyTable()
            mytable.field_names = list_columns

            # форматирование таблицы перед отправкой по email:
            # заменяем 0 пробелами, округляем int и float до 2-х знаков, если больше 1, округляем до 4-х, если меньше 0
            size_frame = len(q_array)
            q_array_format = copy.deepcopy(q_array)
            for i in range(size_frame):
                size_row = len(q_array[i])
                for j in range(size_row):
                    q_array_format[i][j] = get_correct_field(q_array[i][j], len_field)

            # заполнение таблицы
            for i in range(size_frame):
                mytable.add_row(q_array_format[i])

            # преобразование данных в html
            msg = mytable.get_html_string(attributes={"border": 1, "cellpadding": 4,
                                                      "style": "border-width: 1px; border-collapse: collapse;"})
            if msg:
                # отправка на почту
                email_obj = sms.SendMessage(email_box_from, password_email_box,
                                            post_server_name, int(post_server_port))
                email_obj.send_email_html(email_box_to, letter_subject, '', msg)
        else:
            print('Не найдена одна из таблиц: ', name_table_1, ' или ', name_table_2, ' или ', name_table_portf)
            logging.warning('Не найдена одна из таблиц: %s или %s или %s', name_table_1, name_table_2, name_table_portf)


def reduce_position_in_portfolio(curs_obj, upside: float, recommendation: float, delta_price: float):
    func_name = 'reduce_position_in_portfolio '

    # рекоммендации по уменьшению позиции формируем индивидуально для каждого портфеля
    name_portfolio_table_ = parsing_line(name_portfolio_table)
    size_list_portfolio = len(name_portfolio_table_)
    for i in range(size_list_portfolio):
        # тема письма
        letter_subject = 'Уменьшить или закрыть позиции в портфеле ' + name_portfolio_table_[i] + ' с потенциалом ниже '
        letter_subject = letter_subject + str(upside) + ' или уровнем рекоммендаций выше '
        letter_subject = letter_subject + str(recommendation)
        letter_subject = letter_subject + ' и доходностью больше ' + str(delta_price) + ' %'
        letter_subject = letter_subject + ' для БД ' + str(dir_db)
        len_field = 0  # Минимальная ширина колонки для письма

        # название портфеля формируем из заглавных букв
        name_portf = construct_name_portfolio(name_portfolio_table_[i])
        # названия колонок с количествами
        name_column_q_portf = name_portf + '_q'

        # названия таблиц, которые нужны для формирования запроса
        name_table_1 = '_Report_return_high'
        name_table_portf = name_portfolio_table_[i]

        # проверка существования таблиц
        condition_1 = sql_obj.check_table_is_exists(curs_obj, name_table_1)
        condition_3 = sql_obj.check_table_is_exists(curs_obj, name_table_portf)
        if condition_1 and condition_3:
            q = ''' 
                SELECT rrh.Ticker, Ticker_name, Sector, Return_prc, Target, Upside, Recommend, 
                       psl.Quantity_portf, psl.Price_aver, psl.Price_curr, 
                       (psl.Price_curr / psl.Price_aver - 1) * 100 AS Profit
                FROM "_Report_return_high" rrh
                JOIN "{name_portfolio}" psl 
                ON
                rrh.Ticker = psl.Ticker AND 
                psl.Price_curr / psl.Price_aver > {delta_price} AND
                (rrh.Upside < {upside} or rrh.Recommend > {recommendation})
                ORDER BY Upside  
                '''
            q_ = q.format(upside=upside, recommendation=recommendation,
                          name_portfolio=name_portfolio_table_[i],
                          delta_price=str(1 + delta_price / 100))
            # формирование отчета
            curs_obj.execute(q_)
            rows = cursorObj.fetchall()
            # [('YNDX', 'Яндекс', 'Communication Services', 4.82, 5340.22, -6.77, 1.0, 4.0, 4834.5, 5692.0, 17.7370979418761),
            # Ticker, Ticker_name, Sector, Return_prc, Target, Upside, Recommend,
            # Quantity_portf, Price_aver, Price_curr, Profit

            q_array = []
            for row in rows:
                # список строки
                q_row = []
                q_row.append(row[0])
                q_row.append(row[1])
                q_row.append(row[2])
                q_row.append(row[3])
                q_row.append(row[4])
                q_row.append(row[5])
                q_row.append(row[6])
                q_row.append(row[7])
                q_row.append(row[8])
                q_row.append(row[9])
                q_row.append(row[10])
                q_array.append(q_row)

            # получился массив q_array, который нужно отсортировать по потенциалу
            q_array.sort(key=lambda x: x[5])

            # формирование письма и отправка на почту
            list_columns = ['Ticker', 'Ticker_name', 'Sector', 'Return_prc', 'Target', 'Upside', 'Recom',
                            'Q_portf', 'Pr_aver', 'Pr_curr', 'Profit']
            mytable = PrettyTable()
            mytable.field_names = list_columns

            # форматирование таблицы перед отправкой по email:
            # заменяем 0 пробелами, округляем int и float до 2-х знаков, если больше 1, округляем до 4-х, если меньше 0
            size_frame = len(q_array)
            q_array_format = copy.deepcopy(q_array)
            for i in range(size_frame):
                size_row = len(q_array[i])
                for j in range(size_row):
                    q_array_format[i][j] = get_correct_field(q_array[i][j], len_field)

            # заполнение таблицы
            for i in range(size_frame):
                mytable.add_row(q_array_format[i])

            # преобразование данных в html
            msg = mytable.get_html_string(attributes={"border": 1, "cellpadding": 4,
                                                      "style": "border-width: 1px; border-collapse: collapse;"})
            if msg:
                # отправка на почту
                email_obj = sms.SendMessage(email_box_from, password_email_box,
                                            post_server_name, int(post_server_port))
                email_obj.send_email_html(email_box_to, letter_subject, '', msg)
        else:
            print('Не найдена одна из таблиц: ', name_table_1, ' или ', name_table_portf)
            logging.warning('Не найдена одна из таблиц: %s или %s', name_table_1, name_table_portf)


####################################################################################################
logging.basicConfig(filename='screener.log',
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
print('name_main_table : ', name_main_table )
print('index_ticker : ', index_ticker)
print('name_portfolio_table  : ', name_portfolio_table )

filename = path + '\\' + name_db
if print_comment:
    print('Используется БД: ', filename)
logging.info('Используется БД: %s', filename)

sql_obj = sqm.SQLite()

# проверка наличия БД
if sql_obj.check_db(filename) is False:
    if print_comment:
        print("База данных ", filename, " не обнаружена. Завершение работы")
    logging.warning('База данных %s не обнаружена', filename)
    quit()
else:
    if print_comment:
        print("База данных ", filename, " существует")
    logging.info('База данных %s существует', filename)

# устанавливаем соединение и создаем обьект курсора
con = sql_obj.create_connection(filename)
cursorObj = con.cursor()

# проверим существование сводной таблицы _Tickers
# таблица создается вручную и потом заполняется начальными данными по тикерам и биржам
if sql_obj.check_table_is_exists(cursorObj, name_main_table):
    # print("Таблица ", name_main_table, " существует")
    logging.info('Таблица %s существует', name_main_table)
    get_report = True
    # Тип отчета: 0 - письмо, 1 - отчет в БД, 2 - письмо и отчет в БД
    type_report = 1
    # формирование отчета по дивидендной доходности акций
    if get_report:
        print('# формирование отчета по дивидендной доходности акций')
        report_dividends(con, cursorObj, type_report)
    # формирование отчета по потенциальной доходности до максимума года + исторического максимума
    # по недельным графикам
    if get_report:
        print('# формирование отчета по потенциальной доходности до максимума года')
        potential_return_to_high_year(con, cursorObj, type_report)
    # формирование отчета по потенциальной доходности до максимума года
    # в интервале от low_range до high_range с сортировкой по рекоммендации
    low_range = 0
    high_range = 9
    if get_report:
        print('# формирование отчета по потенциальной доходности до максимума года в интервале от 0 до 9')
        potential_return_to_high_year_in_range(con, cursorObj, type_report, low_range, high_range)
    low_range = 9
    high_range = 23
    if get_report:
        print('# формирование отчета по потенциальной доходности до максимума года в интервале от 9 до 23')
        potential_return_to_high_year_in_range(con, cursorObj, type_report, low_range, high_range)
    low_range = 23
    high_range = 38
    if get_report:
        print('# формирование отчета по потенциальной доходности до максимума года в интервале от 23 до 38')
        potential_return_to_high_year_in_range(con, cursorObj, type_report, low_range, high_range)
    low_range = 38
    high_range = 50
    if get_report:
        print('# формирование отчета по потенциальной доходности до максимума года в интервале от 38 до 50')
        potential_return_to_high_year_in_range(con, cursorObj, type_report, low_range, high_range)
    low_range = 50
    high_range = 100
    if get_report:
        print('# формирование отчета по потенциальной доходности до максимума года в интервале от 50 до 100')
        potential_return_to_high_year_in_range(con, cursorObj, type_report, low_range, high_range)
    # формирование отчета список тикеров портфеля с сортировкой по рекоммендациям
    if get_report:
        print('# формирование отчета список тикеров портфеля с сортировкой по рекоммендациям')
        report_recommendations(con, cursorObj, type_report)
    # формирование отчета цена ниже уровня коррекции fibo_level, но выше следующего уровня fibo
    # дата с которой начинаем искать минимум 01.03.2020
    date_start = date(2020, 3, 1)
    fibo_level = 23.6
    next_fibo_level = 38.2
    if get_report:
        print('# формирование отчета цена ниже уровня коррекции 23.6, но выше следующего уровня 38.2')
        report_price_down_correction_level(con, cursorObj, type_report, date_start, fibo_level, next_fibo_level)
    fibo_level = 38.2
    next_fibo_level = 50.0
    if get_report:
        print('# формирование отчета цена ниже уровня коррекции 38.2, но выше следующего уровня 50.0')
        report_price_down_correction_level(con, cursorObj, type_report, date_start, fibo_level, next_fibo_level)
    fibo_level = 50.0
    next_fibo_level = 61.8
    if get_report:
        print('# формирование отчета цена ниже уровня коррекции 50.0, но выше следующего уровня 61.8')
        report_price_down_correction_level(con, cursorObj, type_report, date_start, fibo_level, next_fibo_level)
    fibo_level = 61.8
    next_fibo_level = 100
    if get_report:
        print('# формирование отчета цена ниже уровня коррекции 61.8, но выше следующего уровня 100')
        report_price_down_correction_level(con, cursorObj, type_report, date_start, fibo_level, next_fibo_level)
    # формирование отчета с величиной коррекции к последнему движению
    if get_report:
        print('# формирование отчета с величиной коррекции к последнему движению')
        report_size_correction(con, cursorObj, type_report, date_start)

    # сложные запросы с отправкой на почту, без формирования таблиц в БД
    get_report = True
    if get_report:
        # что нового купить в портфель
        if dir_db == 'usa' or dir_db == 'USA':
            # с upside > 20, рекоммендацией <= 2.5 и потенциалом до максимума года < upside +
            # величина текущей коррекции > 23, все сектора кроме 'Healthcare'
            # дополнительные фильтры только для БД USA:
            # капитализация Big или Large,
            # PE >= 0 and PE < 100,
            # Margin >= 0,
            # Beta < 1.6
            # параметры для дополнительных фильтров получим из таблицы _System_variables
            # внутри функции whats_new_to_buy_usa
            print('# сложные запросы с отправкой на почту whats_new_to_buy_usa')
            whats_new_to_buy_usa(cursorObj, min_upside, max_recommendation, sector_except, 'no equals', 'down',
                             min_size_correct)
            # с upside > 20, рекоммендацией <= 2.5 и потенциалом до максимума года > upside +
            # величина текущей коррекции > 23, все сектора кроме 'Healthcare'
            print('# сложные запросы с отправкой на почту whats_new_to_buy_usa все сектора кроме Healthcare')
            whats_new_to_buy_usa(cursorObj, min_upside, max_recommendation, sector_except, 'no equals', 'up',
                             min_size_correct)
            # с upside > 20, рекоммендацией <= 2.5 и потенциалом до максимума года < upside +
            # величина текущей коррекции > 23, только сектор 'Healthcare'
            print('# сложные запросы с отправкой на почту whats_new_to_buy_usa и потенциалом до максимума года < upside')
            whats_new_to_buy_usa(cursorObj, min_upside, max_recommendation, sector_except, 'equals', 'down',
                             min_size_correct)
            # с upside > 20, рекоммендацией <= 2.5 и потенциалом до максимума года > upside +
            # величина текущей коррекции > 23, только сектор 'Healthcare'
            print(
                '# сложные запросы с отправкой на почту whats_new_to_buy_usa и потенциалом до максимума года > upside')
            whats_new_to_buy_usa(cursorObj, min_upside, max_recommendation, sector_except, 'equals', 'up',
                             min_size_correct)
        else:
            # с upside > 20, рекоммендацией <= 3 и потенциалом до максимума года < upside +
            # величина текущей коррекции > 23, все сектора
            whats_new_to_buy_rus(cursorObj, min_upside_1, max_recommendation_1, sector_except_1, 'equals', 'down',
                             min_size_correct_1)
            # с upside > 20, рекоммендацией <= 3 и потенциалом до максимума года > upside +
            # величина текущей коррекции > 23, все сектора
            whats_new_to_buy_rus(cursorObj, min_upside_1, max_recommendation_1, sector_except_1, 'equals', 'up',
                             min_size_correct_1)

    if get_report:
        # какие позиции нужно увеличить в портфеле
        if dir_db == 'usa' or dir_db == 'USA':
            print('какие позиции нужно увеличить в портфеле no equals')
            increase_position_in_portfolio_usa(cursorObj, min_upside, max_recommendation, sector_except, 'no equals',
                                           min_size_correct, delta_price)
            print('какие позиции нужно увеличить в портфеле equals')
            increase_position_in_portfolio_usa(cursorObj, min_upside, max_recommendation, sector_except, 'equals',
                                           min_size_correct, delta_price)
        else:
            increase_position_in_portfolio_rus(cursorObj, min_upside_1, max_recommendation_1, sector_except_1, 'equals',
                                           min_size_correct_1, delta_price_1)

    # какие позиции нужно уменьшить в портфеле
    if get_report:
        if dir_db == 'usa' or dir_db == 'USA':
            print('какие позиции нужно уменьшить в портфеле')
            reduce_position_in_portfolio(cursorObj, upside_for_sell, recommendation_for_sell, delta_price)
        else:
            reduce_position_in_portfolio(cursorObj, upside_for_sell_1, recommendation_for_sell_1, delta_price_1)

else:
    print("Таблица ", name_main_table, " не существует. Нужно создать ее вручную. Завершение работы")
    logging.warning("Таблица %s не существует. Нужно создать ее вручную", name_main_table)
    con.close()
    quit()
