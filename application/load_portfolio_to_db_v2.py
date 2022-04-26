from portfolio_package import file_operation_modul as fm
from portfolio_package import sqlite_modul as sqm
import configparser
import os
import logging
import pandas as pd
import numpy as np

path_config_file = 'settings_load_portfolio.ini'   # файл с конфигурацией

# путь к файлу портфеля и БД
path = 'C:\DB_TEST'
# файл с портфелем
name = "stocks_portfolio_467408.xlsx"
sheet = 'Портфель акций'  # имя листа
start_row = 3  # стартовая строка

# база данных
name_db = 'usa_market.db'

# имя таблицы портфеля
name_portfolio = 'usa_test'

# имя сводной таблицы
name_main_table = '_Tickers'


# имя файла со сделками
def get_filename_report(filename_portfolio):
    s_1 = filename_portfolio.replace('stocks_portfolio_', '')
    filename_report = 'complex_portfolio_report_' + s_1
    return filename_report


# количество покупок (длина сетки) по тикеру
def get_size_grid(ticker_list_report, operation_list_report, ticker_list, size_grid):
    for i in range(len(ticker_list)):
        count = 0
        for j in range(len(ticker_list_report)):
            if ticker_list_report[j] == ticker_list[i]:
                if operation_list_report[j] == 'Покупка':
                    count = count + 1
                elif operation_list_report[j] == 'Продажа':
                    count = count - 1
                else:
                    pass
        size_grid.append(count)


# функция для загрузки файла конфигурации
def crud_config(path_):
    if not os.path.exists(path_):
        print("Не обнаружен файл конфигурации")
        logging.warning("Не обнаружен файл конфигурации в %s", path_)

    config = configparser.ConfigParser()
    config.read(path_)

    # Читаем значения из конфиг. файла и присваиваем их глобальным переменным
    global path_config_file
    global path
    global name
    global name_db
    global name_portfolio
    global name_main_table

    path = config.get("Settings", "path")
    name = config.get("Settings", "name")
    name_db = config.get("Settings", "name_db")
    name_portfolio = config.get("Settings", "name_portfolio")
    name_main_table = config.get("Settings", "name_main_table")


####################################################################################################
logging.basicConfig(filename='load_portfolio.log',
                    format='[%(asctime)s] [%(levelname)s] => %(message)s',
                    filemode='w',
                    level=logging.DEBUG)

# загрузка параметров из файла конфигурации
crud_config(path_config_file)
print('Load settings from configuration files: ')
print('path: ', path)
print('name : ', name)
print('name_db : ', name_db)
print('name_portfolio : ', name_portfolio)
print('name_main_table : ', name_main_table)

db_name = path + '\\' + name_db
name_table = '_Portf' + '_' + name_portfolio
# имя таблицы с фиксированными суммами для тикеров портфеля
name_table_fix_sum = '_Portf' + '_' + name_portfolio + '_fix_sum'

# чтение файла с информацией по портфелю
# значения столбцов
# 1 - тикер
# 3 - количество
# 4 - средняя цена

# создаем обьект
file_obj = fm.File(path, name)
# последняя строка
end_row = file_obj.GetAmountRows(sheet)
print("Всего строк в файле портфеля: ", end_row)
logging.info("Всего строк в файле портфеля %d", end_row)

# получим данные из файла
ticker_list = file_obj.ReadColumnExcel(sheet, 1, start_row, end_row)
quantity_list = file_obj.ReadColumnExcel(sheet, 3, start_row, end_row)
average_price = file_obj.ReadColumnExcel(sheet, 4, start_row, end_row)

# чтение файла с историей сделок
# filename_report = get_filename_report(name)
# sheet = 'Сделки'  # имя листа
# start_row = 3  # стартовая строка
# file_obj = fm.File(path, filename_report)
# end_row = file_obj.GetAmountRows(sheet)
# print("Всего строк в файле истории сделок: ", end_row)
# logging.info("Всего строк в файле истории сделок %d", end_row)
# ticker_list_report = file_obj.ReadColumnExcel(sheet, 1, start_row, end_row)
# operation_list_report = file_obj.ReadColumnExcel(sheet, 3, start_row, end_row)

# print(ticker_list_report.shape)
# print(operation_list_report.shape)

# количество покупок (длина сетки) по тикеру
# size_grid_list = []
# get_size_grid(ticker_list_report, operation_list_report, ticker_list, size_grid_list)

sql_obj = sqm.SQLite()

# проверка наличия БД
if sql_obj.check_db(db_name) is False:
    print("База данных ", db_name, " не обнаружена")
    logging.warning("База данных %s не обнаружена", db_name)
else:
    print("База данных ", db_name, " существует")
    logging.info("База данных %s существует", db_name)

    # устанавливаем соединение и создаем обьект курсора
    con = sql_obj.create_connection(db_name)
    cursorObj = con.cursor()

    # проверим существование таблицы с фиксированными суммами
    if sql_obj.check_table_is_exists(cursorObj, name_table_fix_sum):
        print("Таблица ", name_table_fix_sum, " существует")
        logging.info("Таблица %s существует", name_table_fix_sum)
    else:
        # таблицы не существует. создадим ее
        q = """ CREATE TABLE {table}
                (
                    Ticker text NOT NULL,
                    Date_update date DEFAULT '1970.01.01',
                    Quantity real DEFAULT 0.0,
                    Price_curr real DEFAULT 0.0,
                    Summ real DEFAULT 0.0
                )
            """
        q_mod = q.format(table=name_table_fix_sum)
        logging.info("Таблица %s не существует. Создадим ее", name_table_fix_sum)
        cursorObj.execute(q_mod)
        con.commit()

    # заполняем или корректируем таблицу с фиксированными суммами
    if sql_obj.check_table_is_exists(cursorObj, name_table_fix_sum):
        # если тикер есть в таблице, ничего не делаем (в случае добора
        # коррекция вручную
        # если тикера нет, добавляем его. сумма = кол-во * текущая цена
        size_frame = len(ticker_list)
        for i in range(size_frame):
            find_value = (ticker_list[i],)
            q_1 = "SELECT {column_1} FROM {table} WHERE {column_1}=?"
            q_1_mod = q_1.format(table=name_table_fix_sum, column_1='Ticker')
            cursorObj.execute(q_1_mod, find_value)
            if cursorObj.fetchone() is None:
                # тикера нет в таблице
                if quantity_list[i] > 0:
                    # получим текущую цену из таблицы дневных котировок
                    name_table_price_day = ticker_list[i] + '_' + '1d'
                    if sql_obj.check_table_is_exists(cursorObj, name_table_price_day):
                        last_close = sql_obj.get_last_value_column(cursorObj, name_table_price_day, "Date",
                                                                   "Close")
                        if last_close is not None:
                            if last_close[1] is not None and last_close[1] != 0:
                                last_close_ = last_close[1]
                                # загружаем данные
                                q = '''INSERT INTO {table} (Ticker, Date_update, Quantity, Price_curr, Summ) 
                                                   VALUES(?, ?, ?, ?, ?)'''
                                q_mod = q.format(table=name_table_fix_sum)
                                cursorObj.execute(q_mod, (ticker_list[i], sql_obj.current_date(), quantity_list[i],
                                                          round(last_close_, 2),
                                                          round(quantity_list[i] * last_close_, 2)))
                                con.commit()
                    else:
                        print("Таблица ", name_table_price_day, " не существует")
                        logging.warning("Таблица %s не существует", name_table_price_day)
                        continue
    else:
        print("Таблица ", name_table_fix_sum, " не существует")
        logging.warning("Таблица %s не существует", name_table_fix_sum)

    # заполняем или корректируем таблицу портфеля
    if sql_obj.check_table_is_exists(cursorObj, name_table):
        print("Таблица ", name_table, " существует")
        logging.info("Таблица %s существует", name_table)
    else:
        # таблицы не существует. создадим ее
        q = """ CREATE TABLE {table}
                (
                    Ticker text NOT NULL,
                    Date_update date DEFAULT '1970.01.01',
                    Quantity_portf real DEFAULT 0.0,
                    Price_aver real DEFAULT 0.0,
                    Sum_portf real DEFAULT 0.0,
                    Quantity_DB real DEFAULT 0.0,
                    Price_curr real DEFAULT 0.0,
                    Sum_DB real DEFAULT 0.0,
                    Delta_vol DEFAULT 0.0,
                    Sum_fix DEFAULT 0.0,
                    Size_grid DEFAULT 0.0
                )
            """
        q_mod = q.format(table=name_table)
        logging.info("Таблица %s не существует. Создадим ее", name_table)
        cursorObj.execute(q_mod)
        con.commit()

    # заполняем таблицу
    if sql_obj.check_table_is_exists(cursorObj, name_table):
        size_frame = len(ticker_list)
        for i in range(size_frame):
            find_value = (ticker_list[i],)
            # проверим наличие тикера в общей таблице
            q_1 = "SELECT {column_1} FROM {table} WHERE {column_1}=?"
            q_1_mod = q_1.format(table=name_main_table, column_1='Ticker')
            cursorObj.execute(q_1_mod, find_value)
            if cursorObj.fetchone() is None:
                if quantity_list[i] != 0:
                    print("Тикера ", ticker_list[i], ' в таблице', name_main_table, ' нет')
                    logging.warning("Тикера %s нет в таблице %s_1", ticker_list[i], name_main_table)

            # если нулевой остаток, значит тикер был полностью распродан
            # удаляем строку из таблицы портфеля и таблицы с фиксированными суммами
            if quantity_list[i] == 0:
                sql_obj.delete_rows_condition(con, cursorObj, name_table, 'Ticker', ticker_list[i])
                sql_obj.delete_rows_condition(con, cursorObj, name_table_fix_sum, 'Ticker', ticker_list[i])
                continue

            # если тикера нет в таблице портфеля, добавляем новую строку
            q_1 = "SELECT {column_1} FROM {table} WHERE {column_1}=?"
            q_1_mod = q_1.format(table=name_table, column_1='Ticker')
            cursorObj.execute(q_1_mod, find_value)
            if cursorObj.fetchone() is None:
                q = '''INSERT INTO {table} (Ticker, Date_update, Quantity_portf, Price_aver, Sum_portf, Size_grid) 
                                   VALUES(?, ?, ?, ?, ?, ?)'''
                q_mod = q.format(table=name_table)
                cursorObj.execute(q_mod, (ticker_list[i], sql_obj.current_date(), quantity_list[i],
                                          round(average_price[i], 2), round(quantity_list[i] * average_price[i], 2),
                                          1))
                con.commit()
            # тикер есть в таблице портфеля, обновляем данные Date_update, Quantity_portf, Price_aver, Sum_portf, Delta
            else:
                # дата
                date_update = sql_obj.current_date()
                sql_obj.update_date_in_cell(con, cursorObj, name_table, 'Date_update', date_update,
                                            'Ticker', ticker_list[i])
                # количество
                sql_obj.update_data_in_cell(con, cursorObj, name_table, 'Quantity_portf', str(quantity_list[i]),
                                            'Ticker', ticker_list[i])
                # цена
                sql_obj.update_data_in_cell(con, cursorObj, name_table, 'Price_aver', str(round(average_price[i], 2)),
                                            'Ticker', ticker_list[i])
                # сумма
                sql_obj.update_data_in_cell(con, cursorObj, name_table, 'Sum_portf',
                                            str(round(quantity_list[i] * average_price[i], 2)),
                                            'Ticker', ticker_list[i])
                # расхождение с текущим остатком БД
                quant_db = sql_obj.get_value_from_table(cursorObj, name_table, 'Quantity_DB', 'Ticker', ticker_list[i])
                if quant_db is not None:
                    if quant_db[0] > 0:
                        delta_vol = quant_db[0] - quantity_list[i]
                        sql_obj.update_data_in_cell(con, cursorObj, name_table, 'Delta_vol',
                                                    str(delta_vol),
                                                    'Ticker', ticker_list[i])
                # длина сетки
                sql_obj.update_data_in_cell(con, cursorObj, name_table, 'Size_grid', str(1),
                                            'Ticker', ticker_list[i])

        # # проверка на выбывшие тикеры
        # # получим список тикеров в таблице портфеля
        # if sql_obj.check_table_is_exists(cursorObj, name_table):
        #     print("Проверка на выбывшие тикеры. Таблица портфеля ", name_table, " существует")
        #     logging.info('Проверка на выбывшие тикеры. Таблица портфеля %s существует', name_table)
        #     q = "SELECT * FROM {table}"
        #     q_mod = q.format(table=name_table)
        #     cursorObj.execute(q_mod)
        #     rows = cursorObj.fetchall()
        #     list_portfolio = []
        #     if rows is not None:
        #         for row in rows:
        #             list_portfolio.append(row[0])
        #     # сравниваем список тикеров портфеля со списком в таблице портфеля,
        #     # если есть несовпадение, то это выбывшие тикеры
        #     delta_list = list(set(list_portfolio) - set(ticker_list))
        #     if len(delta_list) == 0:
        #         # расхождений нет, ничего не делаем
        #         pass
        #     else:
        #         # если расхождения обнаружены удалим строки с соответствующим тикером из
        #         # таблиц портфеля и таблицы с фиксированными суммами
        #         size_list = len(delta_list)
        #         for i in range(size_list):
        #             sql_obj.delete_rows_condition(con, cursorObj, name_table, 'Ticker', delta_list[i])
        #             sql_obj.delete_rows_condition(con, cursorObj, name_table_fix_sum, 'Ticker', delta_list[i])
    else:
        print("Таблица ", name_table, " не существует")
        logging.warning("Таблица %s не существует", name_table)
        con.close()
        quit()

    con.close()
