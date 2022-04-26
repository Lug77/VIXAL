from portfolio_package import sqlite_modul as sqm
from portfolio_package import send_message_modul as sms
import configparser
import logging
import argparse
import os
import pandas as pd
from datetime import datetime
import math

path_config_file = 'settings_expert_system.ini'   # файл с конфигурацией

# аргумент из командной строки для выбора каталога БД
# dir_db = 'RUS'
dir_db = 'USA'

# название БД
path = 'C:\DB_RUS'
name_db = 'rus_market.db'

# имя сводной таблицы
name_main_table = '_Tickers'

# список портфелей
name_portfolio_table = ''

# минимальная сумма для тикера в портфеле
list_min_summ_ticker_in_portfolio = '20000, 20000'

# коэффициенты для расчета шагов через СКО
coeff_in = 2.0
coeff_out = 4.0
# порог снижения цены в процентах от себестоимости, для добора еще 1-го лота, если в остатке 1 лот
level_adding_pos = 25

# отправка сообщений
post_server_name = 'smtp.yandex.ru'  # отправитель
post_server_port = '465'
email_box_from = "your_email"
password_email_box = "password"
email_box_to = "your_email"  # получатель

# вывод сообщений в консоль
print_comment = True


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
    global list_min_summ_ticker_in_portfolio

    # выбираем путь к БД в зависимости от аргумента в командной строке
    if dir_db == 'usa' or dir_db == 'USA':
        path = config.get("Settings", "path")
        name_db = config.get("Settings", "name_db")
        name_portfolio_table = config.get("Settings", "name_portfolio_table")
        list_min_summ_ticker_in_portfolio = config.get("Settings", "list_min_summ_ticker_in_portfolio")
        print('Работаем с Базой 1 path ', path, " name_db ", name_db)
        logging.info('Работаем с Базой 1 path %s name_db %s', path, name_db)
    elif dir_db == 'rus' or dir_db == 'RUS':
        path = config.get("Settings", "path_1")
        name_db = config.get("Settings", "name_db_1")
        name_portfolio_table = config.get("Settings", "name_portfolio_table_1")
        list_min_summ_ticker_in_portfolio = config.get("Settings", "list_min_summ_ticker_in_portfolio_1")
        print('Работаем с Базой 2 path ', path, " name_db ", name_db)
        logging.info('Работаем с Базой 2 path %s name_db %s', path, name_db)
    else:
        # используются значения по умолчанию
        print('Используются значения по умолчанию path ', path, " name_db ", name_db)
        logging.info('Используются значения по умолчанию path %s name_db %s', path, name_db)
        print('Используются значения по умолчанию name_portfolio_table ', name_portfolio_table)
        logging.info('Используются значения по умолчанию name_portfolio_table %s', name_portfolio_table)
        print('Используются значения по умолчанию list_min_summ_ticker_in_portfolio ',
              list_min_summ_ticker_in_portfolio)
        logging.info('Используются значения по умолчанию list_min_summ_ticker_in_portfolio %s',
                     list_min_summ_ticker_in_portfolio)

    global name_main_table
    global coeff_in
    global coeff_out
    global post_server_name
    global post_server_port
    global email_box_from
    global password_email_box
    global email_box_to
    global level_adding_pos

    name_main_table = config.get("Settings", "name_main_table")
    coeff_in = int(config.get("Settings", "coeff_in"))
    coeff_out = int(config.get("Settings", "coeff_out"))
    level_adding_pos = int(config.get("Settings", "level_adding_pos"))
    post_server_name = config.get("Settings", "post_server_name")
    post_server_port = config.get("Settings", "post_server_port")
    email_box_from = config.get("Settings", "email_box_from")
    password_email_box = config.get("Settings", "password_email_box")
    email_box_to = config.get("Settings", "email_box_to")


# функция для расчета СКО тикера
def standart_deviation(sq_obj, cursor, ticker: str):
    # получим DataFrame для тикера за последний год
    name_table_price_day = ticker + '_' + '1d'
    if sql_obj.check_table_is_exists(cursorObj, name_table_price_day):
        data = sq_obj.get_column_value(cursor, name_table_price_day, 'Close', str(31536000))
        if data is not None:
            #[(302.5,), (291.1199951171875,), (282.7300109863281,)
            # преобразование кортежа в series
            data_series = pd.MultiIndex.from_tuples(data).to_frame()[0].reset_index(drop=True)
            # вычисление приращений абсолютных
            # data_series_ = data_series.diff()
            # вычисление приращений относительных
            data_series_ = data_series.pct_change()
            # расчет std
            return data_series_.std()
        else:
            return 0
    else:
        if print_comment:
            print("Таблицы дневных котировок ", name_table_price_day, " не существует")
        logging.warning("Таблицы дневных котировок %s не существует", name_table_price_day)
        return 0


# функция для получения лотности тикера
def get_lot_ticker(sq_obj, cursor, ticker: str):
    # получим лотность для тикера из сводной таблицы
    lot_ticker = sq_obj.get_value_from_table(cursor,
                                             name_main_table,
                                             "Lot",
                                             'Ticker',
                                             ticker)
    if lot_ticker is None:
        if print_comment:
            print('Не указана лотность для тикера ', current_ticker, ' будет использовано 1')
        logging.warning('Не указана лотность для тикера %s будет использовано 1', current_ticker)
        return 1
    else:
        if lot_ticker[0] == 0:
            if print_comment:
                print('Лотность для тикера ', current_ticker, ' =0, будет использовано 1')
            logging.warning('Лотность для тикера %s =0, будет использовано 1', current_ticker)
            return 1
        else:
            return lot_ticker[0]


# функция для получения текущей цены тикера
def get_price_ticker(sq_obj, cursor, ticker: str):
    # текущую цену получим из таблицы часовых котировок
    # проверим существование таблицы для загрузки часовых котировок
    name_table_price_hour = ticker + '_' + '1h'
    if sq_obj.check_table_is_exists(cursor, name_table_price_hour):
        # print("Таблица ", name_table_price_hour, " существует")
        logging.info('Таблица %s существует', name_table_price_hour)
        price_ticker = sq_obj.get_last_value_column(cursor,
                                                    name_table_price_hour,
                                                    'Date',
                                                    'Close')
        if price_ticker is None:
            if print_comment:
                print('Нет последней котировки для тикера ', ticker)
            logging.warning('Нет последней котировки для тикера %s', ticker)
            return 0
        else:
            if price_ticker[1] == 0:
                if print_comment:
                    print('Последняя котировка для тикера ', current_ticker, ' =0')
                logging.warning('Последняя котировка для тикера %s =0', current_ticker)
                return 0
            else:
                return price_ticker[1]
    else:
        if print_comment:
            print("Таблица ", name_table_price_hour, " не обнаружена")
        logging.warning('Таблица %s не обнаружена', name_table_price_hour)
        return 0


# функция для получения фиксированной суммы для тикера
def get_fix_sum_ticker(sq_obj, cursor, ticker: str, name_table: str):
    sum_ticker = sq_obj.get_value_from_table(cursor,
                                             name_table,
                                             "Summ",
                                             'Ticker',
                                             ticker)
    if sum_ticker is None:
        if print_comment:
            print('Нет фиксированной суммы для тикера ', ticker)
        logging.warning('Нет фиксированной суммы для тикера %s', current_ticker)
        return 0
    else:
        if sum_ticker[0] == 0:
            if print_comment:
                print('Фиксированная сумма для тикера ', current_ticker, ' =0')
            logging.warning('Фиксированная сумма для тикера %s =0', current_ticker)
            return 0
        else:
            return sum_ticker[0]


def calculated_step(std: float, coeff: float, quantity: float, lot: float):
    # рассчитаем шаги на вход и выход. если шаг < 1, устанавливаем минимальный =1
    step_ticker = std * 100 * coeff * (quantity / lot) / 100
    step_ticker_r = round(step_ticker, 0)
    if step_ticker_r == 0:
        step_ticker_r = 1
    return step_ticker_r


# сообщение для символа об операции
def message_symbol_operation(ticker: str, direction: str, volume: float, price_ticker: float,
                             quantity_ticker: float, lot_ticker: float):
    msg_text_ = '*'*35 + '\n'
    msg_text_ = msg_text_ + 'Символ: ' + ticker + '\n'
    msg_text_ = msg_text_ + 'Обьем в сделке: ' + str(volume) + '\n'
    msg_text_ = msg_text_ + 'Направление: ' + direction + '\n'
    msg_text_ = msg_text_ + 'Цена: ' + str(price_ticker) + '\n'
    msg_text_ = msg_text_ + 'Время: ' + str(datetime.now()) + '\n'
    if direction == 'sell':
        current_position = quantity_ticker - volume * lot_ticker
    else:
        current_position = quantity_ticker + volume * lot_ticker
    msg_text_ = msg_text_ + 'Итого позиция: ' + str(current_position) + '\n'
    return msg_text_


# дата для формирования имени log-файла
def get_date_log():
    current_date_str = str(datetime.now())
    date_log = current_date_str[0:4] + current_date_str[5:7] + current_date_str[8:10] + '_'
    date_log = date_log + current_date_str[11:13] + current_date_str[14:16] + '00'
    return date_log


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


def control_type_float_value(value):
    if type(value) == str:
        s = value.replace(",", ".")
        return float(s)
    else:
        return value


####################################################################################################
# logging.basicConfig(filename='portfolio_manager_for_db' + '_' + get_date_log() + '_' + dir_db + '.log',
logging.basicConfig(filename='portfolio_manager_for_db' + '_' + dir_db + '.log',
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
print('name_portfolio_table  : ', name_portfolio_table )
print('list_min_summ_ticker_in_portfolio : ', list_min_summ_ticker_in_portfolio)
print('coeff_in : ', coeff_in)
print('coeff_out : ', coeff_out)

filename = path + '\\' + name_db
if print_comment:
    print('Используется БД: ', filename)
logging.info('Используется БД: %s', filename)

sql_obj = sqm.SQLite()

# основной цикл, предполагается однократный запуск с помощью планировщика
if 1 == 1:
    # старт загрузки котировок в БД
    # проверка наличия БД
    if sql_obj.check_db(filename) is False:
        if print_comment:
            print("База данных ", filename, " не обнаружена")
        logging.warning('База данных %s не обнаружена', filename)
        quit()
    else:
        if print_comment:
            print("База данных ", filename, " существует")
        logging.info('База данных %s существует', filename)

    # устанавливаем соединение и создаем обьект курсора
    con = sql_obj.create_connection(filename)
    cursorObj = con.cursor()

    # нужно распарсить строку со списком портфелей
    portf_list = parsing_line(name_portfolio_table)
    # нужно распарсить строку с минимальной суммой для портфелей
    min_summ_portfolio_list = parsing_line(list_min_summ_ticker_in_portfolio)
    # запуск цикла по портфелям
    count = 0
    for name_portfolio_table_ in portf_list:
        if name_portfolio_table_ != "":
            min_summ_portfolio = float(min_summ_portfolio_list[count])
            if print_comment:
                print("Текущий портфель ", name_portfolio_table_, " мин сумма ", min_summ_portfolio)
            logging.info('Текущий портфель %s мин сумма %d', name_portfolio_table_, min_summ_portfolio)
            # проверим существование таблицы портфеля и таблицы с фиксированной суммой для тикеров портфеля
            name_table_fix_sum = name_portfolio_table_ + '_fix_sum'
            cond_table_1 = sql_obj.check_table_is_exists(cursorObj, name_portfolio_table_)
            cond_table_2 = sql_obj.check_table_is_exists(cursorObj, name_table_fix_sum)
            if cond_table_1 and cond_table_2:
                if print_comment:
                    print("Таблицы ", name_portfolio_table_, " и ", name_table_fix_sum, " существуют")
                logging.info('Таблицы %s и %s существует', name_portfolio_table_, name_table_fix_sum)
                # загружаем портфель
                q = "SELECT * FROM {table}"
                q_mod = q.format(table=name_portfolio_table_)
                cursorObj.execute(q_mod)
                rows = cursorObj.fetchall()
                if rows is not None:
                    # портфель не пустой
                    # Ticker, Date_update, Quantity_portf, Price_aver, Sum_portf, Quantity_DB, Price_curr, Sum_DB, Delta_vol
                    # ('LKOH', '2021-09-20 14:05:54', 8.0, 6369.1, 50952.8, 0.0, 0.0, 0.0, 0.0)
                    # запускаем цикл по тикерам портфеля
                    msg_text_ = ''
                    for row in rows:
                        current_ticker = row[0]
                        logging.info('Текущий тикер %s', current_ticker)
                        quantity_ticker = row[2]
                        price_in_ticker = row[3]
                        # сумму начальной настройки получаем из таблицы с фиксированными суммами для тикеров
                        summ_begin_set_ = get_fix_sum_ticker(sql_obj, cursorObj, current_ticker, name_table_fix_sum)
                        summ_begin_set = control_type_float_value(summ_begin_set_)
                        # print('min_summ_portfolio ', min_summ_portfolio, ' type(min_summ_portfolio) ',
                        #       type(min_summ_portfolio))
                        # print('summ_begin_set ', summ_begin_set, ' type(summ_begin_set) ',
                        #       type(summ_begin_set))
                        # если сумма начальной настройки < min_summ_portfolio, используем min_summ_portfolio
                        if summ_begin_set < min_summ_portfolio:
                            summ_begin_set = min_summ_portfolio
                        # добавим сумму начальной настройки в таблицу
                        sql_obj.update_data_in_cell(con, cursorObj, name_portfolio_table_,
                                                    'Sum_fix', str(round(summ_begin_set, 2)), 'Ticker',
                                                    current_ticker)
                        quantity_db = row[5]
                        # получим лотность для тикера из сводной таблицы
                        lot_ticker_ = get_lot_ticker(sql_obj, cursorObj, current_ticker)
                        # получим текущую цену из таблицы часовых котировок и обновим ее в таблицу портфеля
                        price_current_ticker = get_price_ticker(sql_obj, cursorObj, current_ticker)
                        if price_current_ticker > 1:
                            sql_obj.update_data_in_cell(con, cursorObj, name_portfolio_table_,
                                                        'Price_curr', str(round(price_current_ticker, 2)), 'Ticker',
                                                        current_ticker)
                        else:
                            sql_obj.update_data_in_cell(con, cursorObj, name_portfolio_table_,
                                                        'Price_curr', str(round(price_current_ticker, 5)), 'Ticker',
                                                        current_ticker)
                        if price_current_ticker != 0:
                            volume_on_buy = 0
                            volume_on_sell = 0
                            stoimost_min_volume = lot_ticker_ * price_current_ticker
                            if quantity_db == 0:
                                # 1-ый проход после загрузки портфеля
                                if quantity_ticker == 1.0:
                                    # если текущая цена ниже себестоимости на 25% покупаем еще 1 лот
                                    if (price_current_ticker / price_in_ticker) < (1 - level_adding_pos / 100):
                                        volume_on_buy = 1
                                        msg_text_ = msg_text_ + message_symbol_operation(current_ticker,
                                                                                         'add to 2',
                                                                                         volume_on_buy,
                                                                                         price_current_ticker,
                                                                                         quantity_ticker,
                                                                                         lot_ticker_)
                                        date_update = sql_obj.current_date()
                                        sql_obj.update_date_in_cell(con, cursorObj, name_portfolio_table_,
                                                                    'Date_update', date_update, 'Ticker',
                                                                    current_ticker)
                                        # заполняем текущий остаток БД
                                        new_quantity = quantity_ticker + (volume_on_buy - volume_on_sell) * lot_ticker_
                                        sql_obj.update_data_in_cell(con, cursorObj, name_portfolio_table_,
                                                                    'Quantity_DB', str(new_quantity), 'Ticker',
                                                                    current_ticker)
                                        # заполняем сумму БД
                                        sql_obj.update_data_in_cell(con, cursorObj, name_portfolio_table_,
                                                                    'Sum_DB',
                                                                    str(round(price_current_ticker * new_quantity, 2)),
                                                                    'Ticker',
                                                                    current_ticker)
                                        # заполняем разность по количеству портфеля и БД
                                        sql_obj.update_data_in_cell(con, cursorObj, name_portfolio_table_,
                                                                    'Delta_vol',
                                                                    str(new_quantity - quantity_ticker),
                                                                    'Ticker',
                                                                    current_ticker)
                                    else:
                                        # изменений не произошло
                                        date_update = sql_obj.current_date()
                                        sql_obj.update_date_in_cell(con, cursorObj, name_portfolio_table_,
                                                                    'Date_update', date_update, 'Ticker',
                                                                    current_ticker)
                                        sql_obj.update_data_in_cell(con, cursorObj, name_portfolio_table_,
                                                                    'Quantity_DB', str(quantity_ticker), 'Ticker',
                                                                    current_ticker)
                                        sql_obj.update_data_in_cell(con, cursorObj, name_portfolio_table_,
                                                                    'Sum_DB',
                                                                    str(round(price_current_ticker * quantity_ticker, 2)),
                                                                    'Ticker',
                                                                    current_ticker)
                                        sql_obj.update_data_in_cell(con, cursorObj, name_portfolio_table_,
                                                                    'Delta_vol',
                                                                    str(0),
                                                                    'Ticker',
                                                                    current_ticker)
                                else:
                                    # quantity_ticker != 1.0:
                                    stoimost_current = price_current_ticker * quantity_ticker
                                    std_ticker = standart_deviation(sql_obj, cursorObj, current_ticker)
                                    step_in_ticker_r = calculated_step(std_ticker, coeff_in, quantity_ticker,
                                                                       lot_ticker_)
                                    step_out_ticker_r = calculated_step(std_ticker, coeff_out, quantity_ticker,
                                                                        lot_ticker_)
                                    # считаем
                                    if (stoimost_current > 0) and (stoimost_current < summ_begin_set):
                                        # покупка
                                        if (summ_begin_set - stoimost_current) > stoimost_min_volume * step_in_ticker_r:
                                            volume_on_buy = (summ_begin_set - stoimost_current) / stoimost_min_volume
                                            volume_on_buy = math.floor(volume_on_buy)
                                        if volume_on_buy > 0:
                                            # есть обьем на покупку, формируем сообщение для символа
                                            msg_text_ = msg_text_ + message_symbol_operation(current_ticker,
                                                                                             'buy',
                                                                                             volume_on_buy,
                                                                                             price_current_ticker,
                                                                                             quantity_ticker,
                                                                                             lot_ticker_)
                                        # дата обновления позиции
                                        date_update = sql_obj.current_date()
                                        sql_obj.update_date_in_cell(con, cursorObj, name_portfolio_table_,
                                                                    'Date_update', date_update, 'Ticker',
                                                                    current_ticker)
                                        new_quantity = quantity_ticker + (
                                                volume_on_buy - volume_on_sell) * lot_ticker_
                                        # количество
                                        sql_obj.update_data_in_cell(con, cursorObj, name_portfolio_table_,
                                                                    'Quantity_DB', str(new_quantity), 'Ticker',
                                                                    current_ticker)
                                        sql_obj.update_data_in_cell(con, cursorObj, name_portfolio_table_,
                                                                    'Sum_DB',
                                                                    str(round(price_current_ticker * new_quantity, 2)),
                                                                    'Ticker',
                                                                    current_ticker)
                                        sql_obj.update_data_in_cell(con, cursorObj, name_portfolio_table_,
                                                                    'Delta_vol',
                                                                    str(new_quantity - quantity_ticker),
                                                                    'Ticker',
                                                                    current_ticker)
                                    elif stoimost_current > 0 and stoimost_current > summ_begin_set:
                                        # продажа
                                        if (stoimost_current - summ_begin_set) > stoimost_min_volume * step_out_ticker_r:
                                            volume_on_sell = (stoimost_current - summ_begin_set) / stoimost_min_volume
                                            volume_on_sell = math.floor(volume_on_sell)
                                        if volume_on_sell > 0:
                                            # есть обьем на продажу, формируем сообщение для символа
                                            msg_text_ = msg_text_ + message_symbol_operation(current_ticker,
                                                                                             'sell',
                                                                                             volume_on_sell,
                                                                                             price_current_ticker,
                                                                                             quantity_ticker,
                                                                                             lot_ticker_)
                                        # дата обновления позиции
                                        date_update = sql_obj.current_date()
                                        sql_obj.update_date_in_cell(con, cursorObj, name_portfolio_table_,
                                                                    'Date_update', date_update, 'Ticker',
                                                                    current_ticker)
                                        new_quantity = quantity_ticker + (
                                                volume_on_buy - volume_on_sell) * lot_ticker_
                                        # количество
                                        sql_obj.update_data_in_cell(con, cursorObj, name_portfolio_table_,
                                                                    'Quantity_DB', str(new_quantity), 'Ticker',
                                                                    current_ticker)
                                        sql_obj.update_data_in_cell(con, cursorObj, name_portfolio_table_,
                                                                    'Sum_DB',
                                                                    str(round(price_current_ticker * new_quantity, 2)),
                                                                    'Ticker',
                                                                    current_ticker)
                                        sql_obj.update_data_in_cell(con, cursorObj, name_portfolio_table_,
                                                                    'Delta_vol',
                                                                    str(new_quantity - quantity_ticker),
                                                                    'Ticker',
                                                                    current_ticker)
                                    else:
                                        # обновляем дату и переписываем текущий остаток
                                        date_update = sql_obj.current_date()
                                        sql_obj.update_date_in_cell(con, cursorObj, name_portfolio_table_,
                                                                    'Date_update', date_update, 'Ticker',
                                                                    current_ticker)
                                        sql_obj.update_data_in_cell(con, cursorObj, name_portfolio_table_,
                                                                    'Quantity_DB', str(quantity_ticker), 'Ticker',
                                                                    current_ticker)
                                        sql_obj.update_data_in_cell(con, cursorObj, name_portfolio_table_,
                                                                    'Sum_DB',
                                                                    str(round(price_current_ticker * quantity_ticker, 2)),
                                                                    'Ticker',
                                                                    current_ticker)
                                        sql_obj.update_data_in_cell(con, cursorObj, name_portfolio_table_,
                                                                    'Delta_vol',
                                                                    str(0),
                                                                    'Ticker',
                                                                    current_ticker)
                            else:
                                if True:
                                    stoimost_current = price_current_ticker * quantity_db
                                    std_ticker = standart_deviation(sql_obj, cursorObj, current_ticker)
                                    step_in_ticker_r = calculated_step(std_ticker, coeff_in, quantity_db,
                                                                       lot_ticker_)
                                    step_out_ticker_r = calculated_step(std_ticker, coeff_out, quantity_db,
                                                                        lot_ticker_)
                                    # считаем
                                    if (stoimost_current > 0) and (stoimost_current < summ_begin_set):
                                        # покупка
                                        if (summ_begin_set - stoimost_current) > stoimost_min_volume * step_in_ticker_r:
                                            volume_on_buy = (summ_begin_set - stoimost_current) / stoimost_min_volume
                                            volume_on_buy = math.floor(volume_on_buy)
                                        if volume_on_buy > 0:
                                            # есть обьем на покупку, формируем сообщение для символа
                                            msg_text_ = msg_text_ + message_symbol_operation(current_ticker,
                                                                                             'buy',
                                                                                             volume_on_buy,
                                                                                             price_current_ticker,
                                                                                             quantity_db,
                                                                                             lot_ticker_)
                                            # дата обновления позиции
                                            date_update = sql_obj.current_date()
                                            sql_obj.update_date_in_cell(con, cursorObj, name_portfolio_table_,
                                                                        'Date_update', date_update, 'Ticker',
                                                                        current_ticker)
                                            new_quantity = quantity_db + (
                                                    volume_on_buy - volume_on_sell) * lot_ticker_
                                            # количество
                                            sql_obj.update_data_in_cell(con, cursorObj, name_portfolio_table_,
                                                                        'Quantity_DB', str(new_quantity), 'Ticker',
                                                                        current_ticker)
                                            sql_obj.update_data_in_cell(con, cursorObj, name_portfolio_table_,
                                                                        'Sum_DB',
                                                                        str(round(price_current_ticker * new_quantity, 2)),
                                                                        'Ticker',
                                                                        current_ticker)
                                            sql_obj.update_data_in_cell(con, cursorObj, name_portfolio_table_,
                                                                        'Delta_vol',
                                                                        str(new_quantity - quantity_ticker),
                                                                        'Ticker',
                                                                        current_ticker)
                                        else:
                                            # обьем меньше минимального, обновляем сумму и дельту
                                            sql_obj.update_data_in_cell(con, cursorObj, name_portfolio_table_,
                                                                        'Sum_DB',
                                                                        str(round(price_current_ticker * quantity_db,
                                                                                  2)),
                                                                        'Ticker',
                                                                        current_ticker)
                                            sql_obj.update_data_in_cell(con, cursorObj, name_portfolio_table_,
                                                                        'Delta_vol',
                                                                        str(quantity_db - quantity_ticker),
                                                                        'Ticker',
                                                                        current_ticker)
                                    elif stoimost_current > 0 and stoimost_current > summ_begin_set:
                                        # продажа
                                        if (stoimost_current - summ_begin_set) > stoimost_min_volume * step_out_ticker_r:
                                            volume_on_sell = (stoimost_current - summ_begin_set) / stoimost_min_volume
                                            volume_on_sell = math.floor(volume_on_sell)
                                        if volume_on_sell > 0:
                                            # есть обьем на продажу, формируем сообщение для символа
                                            msg_text_ = msg_text_ + message_symbol_operation(current_ticker,
                                                                                             'sell',
                                                                                             volume_on_sell,
                                                                                             price_current_ticker,
                                                                                             quantity_db,
                                                                                             lot_ticker_)
                                            # дата обновления позиции
                                            date_update = sql_obj.current_date()
                                            sql_obj.update_date_in_cell(con, cursorObj, name_portfolio_table_,
                                                                        'Date_update', date_update, 'Ticker',
                                                                        current_ticker)
                                            new_quantity = quantity_db + (
                                                    volume_on_buy - volume_on_sell) * lot_ticker_
                                            # количество
                                            sql_obj.update_data_in_cell(con, cursorObj, name_portfolio_table_,
                                                                        'Quantity_DB', str(new_quantity), 'Ticker',
                                                                        current_ticker)
                                            sql_obj.update_data_in_cell(con, cursorObj, name_portfolio_table_,
                                                                        'Sum_DB',
                                                                        str(round(price_current_ticker * new_quantity, 2)),
                                                                        'Ticker',
                                                                        current_ticker)
                                            sql_obj.update_data_in_cell(con, cursorObj, name_portfolio_table_,
                                                                        'Delta_vol',
                                                                        str(new_quantity - quantity_ticker),
                                                                        'Ticker',
                                                                        current_ticker)
                                        else:
                                            # обьем меньше минимального, обновляем сумму и дельту
                                            sql_obj.update_data_in_cell(con, cursorObj, name_portfolio_table_,
                                                                        'Sum_DB',
                                                                        str(round(price_current_ticker * quantity_db,
                                                                                  2)),
                                                                        'Ticker',
                                                                        current_ticker)
                                            sql_obj.update_data_in_cell(con, cursorObj, name_portfolio_table_,
                                                                        'Delta_vol',
                                                                        str(quantity_db - quantity_ticker),
                                                                        'Ticker',
                                                                        current_ticker)
                                    else:
                                        # изменений в обьеме не произошло, обновляем сумму и дельту
                                        # date_update = sql_obj.current_date()
                                        # sql_obj.update_date_in_cell(con, cursorObj, name_portfolio_table_,
                                        #                             'Date_update', date_update, 'Ticker',
                                        #                             current_ticker)
                                        sql_obj.update_data_in_cell(con, cursorObj, name_portfolio_table_,
                                                                    'Sum_DB',
                                                                    str(round(price_current_ticker * quantity_db, 2)),
                                                                    'Ticker',
                                                                    current_ticker)
                                        sql_obj.update_data_in_cell(con, cursorObj, name_portfolio_table_,
                                                                    'Delta_vol',
                                                                    str(quantity_db - quantity_ticker),
                                                                    'Ticker',
                                                                    current_ticker)
                        else:
                            if print_comment:
                                print('Текущая цена для тикера ', current_ticker, ' = 0')
                            logging.warning('Текущая цена для тикера %s = 0', current_ticker)
                            continue

                    # конец цикла прохода по портфелю
                    # если сообщение непустое, отправляем его
                    if msg_text_ != '':
                        msg_subject = 'Коррекция портфеля ' + name_portfolio_table_
                        email_obj = sms.SendMessage(email_box_from, password_email_box, post_server_name,
                                                    int(post_server_port))
                        email_obj.send_email(email_box_to, msg_subject, msg_text_)
                        if print_comment:
                            print(msg_text_)
                        logging.info('%s', msg_text_)
            else:
                if print_comment:
                    print("Таблицы портфеля ", name_portfolio_table_, " или таблицы с фикс суммами ",
                          name_table_fix_sum, " не существует")
                logging.warning('Таблицы портфеля %s или таблицы с фикс суммами %s не существует',
                                name_portfolio_table_, name_table_fix_sum)
        # следующий портфель
        count = count + 1
