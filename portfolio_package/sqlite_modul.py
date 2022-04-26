from dataclasses import dataclass
import sqlite3 as sl
from sqlite3 import Error
from datetime import datetime, date, time
import os
import requests
import xml.etree.ElementTree as ET


@dataclass()
class SQLite:

    # подключение пути к БД
    def check_db(self, filename: str):
        return os.path.exists(filename)

    # подключение к БД
    def create_connection(self, path: str):
        connection = None
        try:
            connection = sl.connect(path)
            print('Connection to SQLite DB successful')
        except Error as e:
            print(f"The error'{e}' occured")
        return connection

    # проверка существования таблицы
    def check_table_is_exists(self, curs_obj, table_name: str):
        curs_obj.execute("SELECT name from sqlite_master WHERE type = 'table' AND name = :name_table",
                        {"name_table": table_name})
        # если возвращается не пустой массив, значит таблица есть
        if len(curs_obj.fetchall()) > 0:
            return True
        else:
            return False

    # создание таблицы для загрузки котировок
    def create_table_market_data(self, conn_obj, curs_obj, table_name: str):
        # Ticker, text, нет в ответе на запрос
        # Per, text, нет в ответе на запрос
        # Date, date, извлекаем из Date запроса date и time и комбинируем их в дату со временем
        # Open, real
        # High, real
        # Low, real
        # Close, real
        # Close_adj, real
        # Volume, integer

        q = """
        CREATE TABLE {table} (
        Ticker text, Per text, Date date, Open real, High real, Low real, Close real, Close_adj real, Volume real
        )
        """
        curs_obj.execute(q.format(table=table_name))
        conn_obj.commit()

    # создание таблицы для отчета по дивидендной доходности
    def create_table_dividend_report(self, conn_obj, curs_obj, table_name: str,
                                     list_column: list, list_type: list):
        size_list = len(list_column)
        q_ = '(Ticker_ID integer PRIMARY KEY, '
        for i in range(size_list):
            if i != size_list-1:
                q_ = q_ + list_column[i] + ' ' + list_type[i] + ', '
            else:
                q_ = q_ + list_column[i] + ' ' + list_type[i]
        q_ = q_ + ')'
        q = '''CREATE TABLE {table} {list_columns}'''
        q_mod = q.format(table=table_name, list_columns=q_)
        curs_obj.execute(q_mod)
        conn_obj.commit()

    # удаление таблицы
    def delete_table(self, conn_obj, curs_obj, table_name: str):
        q = 'DROP table if exists {table}'
        curs_obj.execute(q.format(table=table_name))
        conn_obj.commit()

    # загрузка одной строки
    def insert_one_rows(self, conn_obj, curs_obj, table_name: str, data: tuple):
        q = "INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)"
        q_ = q.format(table=table_name)
        curs_obj.execute(q_, data)
        conn_obj.commit()

    # множественная загрузка строк
    def insert_many_rows(self, conn_obj, curs_obj, table_name: str, data: list):
        q = "INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)"
        q_ = q.format(table=table_name)
        curs_obj.executemany(q_, data)
        conn_obj.commit()

    # обновление данных в ячейке
    def update_data_in_cell(self, conn_obj, curs_obj, table_name: str, column_name: str,
                            value, column_where_name: str, key: str):
        q = '''UPDATE {table}
              SET {column} = {new_value}
              WHERE {column_where}=?'''
        q_ = q.format(table=table_name, column=column_name, new_value=value, column_where=column_where_name)
        t = (key,)
        curs_obj.execute(q_, t)
        conn_obj.commit()

    # обновление даты и текстовых данных в ячейке
    def update_date_in_cell(self, conn_obj, curs_obj, table_name: str, column_name: str,
                            value: str, column_where_name: str, key: str):
        q = '''UPDATE {table} 
              SET {column} = '{new_value}'
              WHERE {column_where}=?'''
        q_ = q.format(table=table_name, column=column_name, new_value=value, column_where=column_where_name)
        t = (key,)
        curs_obj.execute(q_, t)
        conn_obj.commit()

    # удаление всех строк
    def delete_all_rows(self, conn_obj, curs_obj, table_name: str):
        q = "DELETE FROM {table}"
        q_ = q.format(table=table_name)
        curs_obj.execute(q_)
        conn_obj.commit()

    # удаление строки с условием
    def delete_rows_condition(self, conn_obj, curs_obj, table_name: str, column_name: str, value_condition):
        q = '''DELETE FROM {table}
               WHERE {column} = ?'''
        q_ = q.format(table=table_name, column=column_name)
        t = (value_condition,)
        curs_obj.execute(q_, t)
        conn_obj.commit()

    # удаление строки с условием по дате
    def delete_rows_datetime_condition(self, conn_obj, curs_obj, table_name: str, column_date_name: str,
                                       date_: str, time_: str):
        # формат даты и времени 2021-08-30 12:30:00
        if time_ == '' or time_ == '00:00:00':
            # указана только дата в условии
            q = '''DELETE FROM {table}
                   WHERE {column} >= date('{date_filtr}')'''
            q_ = q.format(table=table_name, column=column_date_name, date_filtr=date_)
            # print(q_)
            curs_obj.execute(q_)
            conn_obj.commit()
        else:
            # указаны дата и время в условии
            q = '''DELETE FROM {table}
                   WHERE {column} >= datetime('{datetime_filtr}')'''
            datetime_filtr_ = date_ + ' ' + time_
            q_ = q.format(table=table_name, column=column_date_name, datetime_filtr=datetime_filtr_)
            # print(q_)
            curs_obj.execute(q_)
            conn_obj.commit()

    # запрос по дате и времени к сводной таблице
    def select_date_time(self, curs_obj, table_name: str, date_, time_):
        # собираем кортеж для запроса
        find_value = datetime.combine(date_, time_)
        t = (find_value,)
        q = "SELECT Date, Open, High, Low, Close, Close_adj, Volume FROM {table} WHERE Date=?"
        q_ = q.format(table=table_name)
        curs_obj.execute(q_, t)
        # на выходе список с одним найденным значением или None, если значение не найдено
        return curs_obj.fetchone()

    # общий запрос по дате и времени к таблице
    def select_date_time_(self, curs_obj, table_name: str, column_name: str, date_, time_):
        # собираем кортеж для запроса
        find_value = datetime.combine(date_, time_)
        t = (find_value,)
        q = "SELECT * FROM {table} WHERE {column}=?"
        q_ = q.format(table=table_name, column=column_name)
        curs_obj.execute(q_, t)
        # на выходе список с одним найденным значением или None, если значение не найдено
        return curs_obj.fetchone()

    # текущая дата в нужном формате
    def current_date(self):
        current_date = datetime.now()
        year = current_date.year
        month = current_date.month
        day = current_date.day
        hour = current_date.hour
        minute = current_date.minute
        second = current_date.second
        return datetime(year, month, day, hour, minute, second)

    # запрос - получить максимальное значение столбца
    def get_max_column_value_no_condition(self, curs_obj, table_name: str, column_name: str):
        q = '''SELECT MAX({column})
               FROM {table}'''
        q_ = q.format(table=table_name, column=column_name)
        curs_obj.execute(q_)
        # на выходе список с одним найденным значением или None, если значение не найдено
        return curs_obj.fetchone()

    # запрос - получить максимальное значение столбца с условием по дате (в интервале
    # сегодня - сегодня - кол-во секунд
    def get_max_column_value(self, curs_obj, table_name: str, column_name: str, shift: str):
        q = '''SELECT MAX({column})
               FROM {table} 
               WHERE Date > datetime('now', '-{shift_} seconds')'''
        q_ = q.format(table=table_name, column=column_name, shift_=shift)
        curs_obj.execute(q_)
        # на выходе список с одним найденным значением или None, если значение не найдено
        return curs_obj.fetchone()

    # запрос - получить минимальное значение столбца с условием по дате (в интервале
    # сегодня - сегодня - кол-во секунд
    def get_min_column_value(self, curs_obj, table_name: str, column_name: str, shift: str):
        q = '''SELECT MIN({column})
               FROM {table} 
               WHERE Date > datetime('now', '-{shift_} seconds')'''
        q_ = q.format(table=table_name, column=column_name, shift_=shift)
        curs_obj.execute(q_)
        # на выходе список с одним найденным значением или None, если значение не найдено
        return curs_obj.fetchone()

    # запрос - получить минимальное значение столбца с условием по дате (в интервале
    # сегодня - сегодня - кол-во секунд
    # и дату минимума
    def get_min_column_value_and_date(self, curs_obj, table_name: str, column_name: str, shift: str):
        q = '''SELECT MIN({column}), Date
               FROM {table} 
               WHERE Date > datetime('now', '-{shift_} seconds')'''
        q_ = q.format(table=table_name, column=column_name, shift_=shift)
        curs_obj.execute(q_)
        return curs_obj.fetchall()

    # запрос - получить последнее значение столбца и дату
    def get_last_value_column(self, curs_obj, table_name: str,
                              column_date_name: str, column_name: str):
        q = '''SELECT MAX({column_date}), {column}
               FROM {table}'''
        q_ = q.format(table=table_name, column_date=column_date_name, column=column_name)
        curs_obj.execute(q_)
        # на выходе список с одним найденным значением или None, если значение не найдено
        return curs_obj.fetchone()

    # запрос - получить последнюю строку из таблицы с котировками
    def get_last_row(self, curs_obj, table_name: str,
                     column_date_name: str):
        q = '''SELECT MAX({column_date}), Date, Open, High, Low, Close, Close_adj, Volume
               FROM {table}'''
        q_ = q.format(table=table_name, column_date=column_date_name)
        curs_obj.execute(q_)
        # на выходе список с одним найденным значением или None, если значение не найдено
        return curs_obj.fetchone()

    # запрос - получить авторекоммендацию по тикеру
    def get_recommendation_avto (self, curs_obj, table_name: str, ticker_name: str):
        q = "SELECT Recom_avto FROM {table} WHERE Ticker=?"
        q_ = q.format(table=table_name)
        t = (ticker_name,)
        curs_obj.execute(q_, t)
        # на выходе список с одним найденным значением или None, если значение не найдено
        return curs_obj.fetchone()

    # запрос - получить сектор для тикера
    def get_sector (self, curs_obj, table_name: str, ticker_name: str):
        q = "SELECT Sector FROM {table} WHERE Ticker=?"
        q_ = q.format(table=table_name)
        t = (ticker_name,)
        curs_obj.execute(q_, t)
        # на выходе список с одним найденным значением или None, если значение не найдено
        return curs_obj.fetchone()

    # запрос - получить полное имя для тикера
    def get_full_name (self, curs_obj, table_name: str, ticker_name: str):
        q = "SELECT Ticker_name FROM {table} WHERE Ticker=?"
        q_ = q.format(table=table_name)
        t = (ticker_name,)
        curs_obj.execute(q_, t)
        # на выходе список с одним найденным значением или None, если значение не найдено
        return curs_obj.fetchone()

    # запрос - получить данные из указанного столбца, указанной таблицы по ключу
    def get_value_from_table (self, curs_obj, table_name: str, column_name: str,
                              column_where_name: str, key: str):
        q = "SELECT {column} FROM {table} WHERE {column_where}=?"
        q_ = q.format(column=column_name, table=table_name, column_where=column_where_name)
        t = (key,)
        curs_obj.execute(q_, t)
        # на выходе список с одним найденным значением или None, если значение не найдено
        return curs_obj.fetchone()

    # запрос - получить количество акций по тикеру
    def get_quantity_ticker (self, curs_obj, table_name: str, ticker_name: str):
        q = "SELECT Quantity_portf FROM {table} WHERE Ticker=?"
        q_ = q.format(table=table_name)
        t = (ticker_name,)
        curs_obj.execute(q_, t)
        # на выходе список с одним найденным значением или None, если значение не найдено
        return curs_obj.fetchone()

    # запрос - получить количество акций и сумму по тикеру
    def get_quantity_summ_ticker (self, curs_obj, table_name: str, ticker_name: str):
        q = "SELECT Quantity_portf, Sum_portf FROM {table} WHERE Ticker=?"
        q_ = q.format(table=table_name)
        t = (ticker_name,)
        curs_obj.execute(q_, t)
        # на выходе список с одним найденным значением или None, если значение не найдено
        return curs_obj.fetchone()

    # запрос - получить количество строк в столбце
    def get_rows (self, curs_obj, table_name: str, column_name: str):
        q = "SELECT COUNT({column}) FROM {table}"
        q_ = q.format(table=table_name, column=column_name)
        curs_obj.execute(q_)
        # на выходе список с одним найденным значением или None, если значение не найдено
        return curs_obj.fetchone()

    # запрос с JOIN получить список тикеров портфеля с рекоммендацией
    def recommend_portf_tickers (self, curs_obj, table_a: str, table_b: str):
        q = ''' 
            SELECT {table_a}.Ticker , {table_a}.Quantity_portf , {table_b}.Recom_avto 
            FROM {table_a} 
            INNER JOIN {table_b}
            ON {table_a}.Ticker = {table_b}.Ticker
            ORDER BY {table_b}.Recom_avto
            '''
        q_ = q.format(table_a=table_a, table_b=table_b)
        curs_obj.execute(q_)
        return curs_obj.fetchall()

    # запрос - получить выборку из столбца с условием по дате (в интервале
    # сегодня - сегодня - кол-во секунд
    def get_column_value(self, curs_obj, table_name: str, column_name: str, shift: str):
        q = '''SELECT {column}
               FROM {table} 
               WHERE Date > datetime('now', '-{shift_} seconds')'''
        q_ = q.format(table=table_name, column=column_name, shift_=shift)
        curs_obj.execute(q_)
        return curs_obj.fetchall()

    # запрос - получить котировки с условием по дате (в интервале
    # сегодня - сегодня - кол-во секунд
    def get_rates(self, curs_obj, table_name: str, shift: str):
        q = '''SELECT *
               FROM {table} 
               WHERE Date > datetime('now', '-{shift_} seconds')'''
        q_ = q.format(table=table_name, shift_=shift)
        curs_obj.execute(q_)
        return curs_obj.fetchall()

    def get_rates_interval(self, curs_obj, table_name: str, start: str, end: str):
        q = '''SELECT *
               FROM {table} 
               WHERE Date >= date('{date_start}') AND Date <= date('{date_end}')'''
        q_ = q.format(table=table_name, date_start=start, date_end=end)
        curs_obj.execute(q_)
        return curs_obj.fetchall()

    # получение курса доллара и евро на указанную дату с сайта cbr.ru
    def get_exchange_rates(self, day: int, month: int, year: int):
        """
        Выполняет запрос к API Банка России.
        :param day: Выбранный день.
        :param month: Выбранный номер месяца.
        :param year: Выбранный код
        :return: dict
        """

        result = {
            'usd': 0,
            'eur': 0,
        }

        if int(day) < 10:
            day = '0%s' % day
        if int(month) < 10:
            month = '0%s' % month

        try:
            # Выполняем запрос к API.
            get_xml = requests.get(
                'http://www.cbr.ru/scripts/XML_daily.asp?date_req=%s/%s/%s' % (day, month, year)
            )

            # Парсинг XML используя ElementTree
            structure = ET.fromstring(get_xml.content)
        except:
            return result

        try:
            # Поиск курса доллара (USD ID: R01235)
            dollar = structure.find("./*[@ID='R01235']/Value")
            result['dollar'] = dollar.text.replace(',', '.')
        except:
            result['dollar'] = 'x'

        try:
            # Поиск курса евро (EUR ID: R01239)
            euro = structure.find("./*[@ID='R01239']/Value")
            result['euro'] = euro.text.replace(',', '.')
        except:
            result['euro'] = 'x'

        # {'usd': 0, 'eur': 0, 'dollar': '78.7847', 'euro': '92.4302'}
        return result

    # перевод значения дивиденда из str в float с учетом валюты дивиденда
    def convert_str_to_float_dividend(self, value: str, date_: datetime):
        value_float = 0
        try:
            value_float = float(value)
            return value_float
        except:
            mark = value.find('$')
            if mark >= 0:
                size = len(value)
                if mark < size:
                    x_ = value[mark + 1:size]
                    day_ = date_.day
                    month_ = date_.month
                    year_ = date_.year
                    rate_usd = self.get_exchange_rates(day_, month_, year_)
                    try:
                        rate_usd_ = rate_usd['dollar']
                        value_float = float(x_)
                        value_float = value_float * float(rate_usd_)
                        return value_float
                    except:
                        return 0
            else:
                return 0

    # запрос - получить значение системной переменной
    def get_system_var_value (self, curs_obj, var_name: str, type_var: str):
        # проверим существование таблицы
        if type_var == 'date':
            q = "SELECT Value_date FROM {table} WHERE Name_var=?"
            q_ = q.format(table='_System_variables')
            t = (var_name,)
            curs_obj.execute(q_, t)
            # на выходе список с одним найденным значением или None, если значение не найдено
            result = curs_obj.fetchone()
            if result is not None:
                return result[0]
            else:
                return 0
        elif type_var == 'text':
            q = "SELECT Value_text FROM {table} WHERE Name_var=?"
            q_ = q.format(table='_System_variables')
            t = (var_name,)
            curs_obj.execute(q_, t)
            result = curs_obj.fetchone()
            if result is not None:
                return result[0]
            else:
                return 0
        elif type_var == 'real':
            q = "SELECT Value_real FROM {table} WHERE Name_var=?"
            q_ = q.format(table='_System_variables')
            t = (var_name,)
            curs_obj.execute(q_, t)
            result = curs_obj.fetchone()
            if result is not None:
                return result[0]
            else:
                return 0
        else:
            return 0

    # запрос - обновить дату проверки значения системной переменной
    def set_date_control_var (self, conn_obj, curs_obj, value, var_name: str, type_var: str):
        if type_var == 'date':
            q = '''UPDATE {table} 
                          SET {column} = '{new_value}'
                          WHERE {column_where}=?'''
            q_ = q.format(table='_System_variables', column='Date_control_var', new_value=value,
                          column_where='Name_var')
            t = (var_name,)
            curs_obj.execute(q_, t)
            conn_obj.commit()

    # запрос - обновить значение системной переменной
    def set_system_var_value (self, conn_obj, curs_obj, value, var_name: str, type_var: str):
        if type_var == 'date':
            q = '''UPDATE {table} 
                          SET {column} = '{new_value}'
                          WHERE {column_where}=?'''
            q_ = q.format(table='_System_variables', column='Value_date', new_value=value,
                          column_where='Name_var')
            t = (var_name,)
            curs_obj.execute(q_, t)
            conn_obj.commit()
        elif type_var == 'text':
            q = '''UPDATE {table} 
                          SET {column} = '{new_value}'
                          WHERE {column_where}=?'''
            q_ = q.format(table='_System_variables', column='Value_text', new_value=value,
                          column_where='Name_var')
            t = (var_name,)
            curs_obj.execute(q_, t)
            conn_obj.commit()
        elif type_var == 'real':
            q = '''UPDATE {table} 
                          SET {column} = {new_value}
                          WHERE {column_where}=?'''
            q_ = q.format(table='_System_variables', column='Value_real', new_value=value,
                          column_where='Name_var')
            t = (var_name,)
            curs_obj.execute(q_, t)
            conn_obj.commit()
        else:
            pass

    # запрос - сделать запись в системный журнал
    def write_event_in_system_log (self, conn_obj, curs_obj, date_event, name_prog: str, comment: str, code: int):
        data = (date_event, name_prog, comment, code)
        q = "INSERT INTO {table} VALUES(?, ?, ?, ?)"
        q_ = q.format(table='_System_log')
        curs_obj.execute(q_, data)
        conn_obj.commit()





