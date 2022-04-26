from portfolio_package import sqlite_modul as sqm
import pandas as pd
import sqlite3 as sl


class AnalystModul:

    def __init__(self, path, name_db, name_main_table, index_ticker):
        self.path = path
        self.name_db = name_db
        self.name_main_table = name_main_table
        self.index_ticker = index_ticker

    def get_target_price(self, sql_obj, curs_obj, table_name: str):
        if sql_obj.check_table_is_exists(curs_obj, table_name):
            q = '''SELECT MAX({column_date}), {column}
                           FROM {table}'''
            q_ = q.format(table=table_name, column_date='Date_update', column='Target')
            curs_obj.execute(q_)
            # на выходе список с одним найденным значением или None, если значение не найдено
            result = curs_obj.fetchone()
            if result is None:
                return 0
            else:
                return result[1]
        return 0

    def calculated_average_price(self, list_target: list):
        # цены в списке начинаются с индекса 2
        size_list = len(list_target)
        temp_list = list_target[2:size_list]
        size_frame = len(temp_list)
        summ = 0
        count = 0
        result = 0
        if size_frame == 0:
            pass
        elif size_frame == 1:
            # одна цена в списке
            result = temp_list[0]
        elif size_frame == 2:
            # две цены в списке
            for i in range(size_frame):
                if temp_list[i] != 0:
                    count = count + 1
                    summ = summ + temp_list[i]
            if count == 0:
                result = 0
            else:
                result = summ / count
        else:
            # три и более цен в списке
            # получим кол-во не нулевых значений
            value_no_zero = self.no_zero_value_in_list(temp_list)
            if value_no_zero >= 3:
                my_series = pd.Series(temp_list)
                ex = my_series.quantile(0.75)
                for i in range(size_frame):
                    if my_series[i] < ex and my_series[i] != 0:
                        count = count + 1
                        summ = summ + my_series[i]
                if count == 0:
                    result = 0
                else:
                    result = summ / count
            else:
                for i in range(size_frame):
                    if temp_list[i] != 0:
                        count = count + 1
                        summ = summ + temp_list[i]
                if count == 0:
                    result = 0
                else:
                    result = summ / count

        if result == 0:
            return result
        elif (result > 0) and (result < 1):
            return round(result, 5)
        else:
            return round(result, 2)

    def no_zero_value_in_list(self, list_):
        size_list = len(list_)
        count = 0
        for i in range(size_list):
            if list_[i] != 0:
                count = count + 1
        return count

    def manipulation_targets(self):
        filename = self.path + '\\' + self.name_db
        sql_obj = sqm.SQLite()

        # проверка наличия БД
        if sql_obj.check_db(filename) is False:
            print("База данных ", filename, " не обнаружена")
            quit()
        else:
            print("База данных ", filename, " существует")

        # устанавливаем соединение и создаем обьект курсора
        # con = sql_obj.create_connection(filename)
        print(filename)
        con = sl.connect(filename)
        cursorObj = con.cursor()

        # проверим существование таблицы для сравнения цен
        name_table_compare_price = '_Compare_target'
        if sql_obj.check_table_is_exists(cursorObj, name_table_compare_price):
            print("Таблица ", name_table_compare_price, " существует")
            # удаляем все строки
            sql_obj.delete_all_rows(con, cursorObj, name_table_compare_price)
        else:
            # если нет, создадим ее
            q = """ CREATE TABLE {table}
                    (
                    Ticker text NOT NULL,
                    Date_update date DEFAULT '1970.01.01',
                    Target_yahoo real DEFAULT 0,
                    Target_bcs real DEFAULT 0,
                    Target_arsagera real DEFAULT 0,
                    Target_average real DEFAULT 0
                    )
                    """
            q_mod = q.format(table=name_table_compare_price)
            # print(q_mod)
            cursorObj.execute(q_mod)
            con.commit()

        # проверим существование сводной таблицы Tickers
        if sql_obj.check_table_is_exists(cursorObj, self.name_main_table):
            # print("Таблица ", name_main_table, " существует")
            # организуем цикл по всем тикерам из сводной таблицы
            q = "SELECT * FROM {table}"
            q_mod = q.format(table=self.name_main_table)
            cursorObj.execute(q_mod)
            rows = cursorObj.fetchall()
            for row in rows:
                # перебор начинаем с указанного индекса (сраниваем с primary key)
                # if row[0] >= self.index_ticker:
                if 1 == 1:
                    current_ticker = row[1]
                    # print(row[1])
                    # имена таблиц с таргетами для базы rus
                    # цены в результате парсинга сайта Yahoo
                    name_table_1 = current_ticker + '_Target_trend'
                    # цены в результате парсинга сайта БКС
                    name_table_2 = current_ticker + '_Target_trend' + '_bcs'
                    # цены в результате парсинга сайта Арсагера
                    name_table_3 = current_ticker + '_Target_trend' + '_arsagera'

                    # если таблица существует, забираем из нее последнюю цену
                    q_row = []
                    q_row.append(current_ticker)
                    q_row.append(sql_obj.current_date())

                    if sql_obj.check_table_is_exists(cursorObj, name_table_1):
                        q_row.append(self.get_target_price(sql_obj, cursorObj, name_table_1))
                    else:
                        q_row.append(0)
                    if sql_obj.check_table_is_exists(cursorObj, name_table_2):
                        q_row.append(self.get_target_price(sql_obj, cursorObj, name_table_2))
                    else:
                        q_row.append(0)
                    if sql_obj.check_table_is_exists(cursorObj, name_table_3):
                        q_row.append(self.get_target_price(sql_obj, cursorObj, name_table_3))
                    else:
                        q_row.append(0)

                    # вычисляем среднюю цену с фильтрацией выбросов
                    aver_price = self.calculated_average_price(q_row)
                    q_row.append(aver_price)

                    # записываем строку в таблицу _Compare_target
                    q = "INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?)"
                    q_ = q.format(table=name_table_compare_price)
                    cursorObj.execute(q_, q_row)
                    con.commit()

                    # обновляем ячейку Target в _Tickers
                    sql_obj.update_data_in_cell(con, cursorObj, self.name_main_table,
                                                'Target', str(aver_price), 'Ticker',
                                                current_ticker)
