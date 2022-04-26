# загрузка иформации с Yahoo через API
# yahoo_agent_info использует пакет yahoo_finance который не отдает
# информацию о полном имени тикера, секторе, индустрии, текущем рейтинге аналитиков
import requests
from requests.exceptions import InvalidURL
from requests.exceptions import HTTPError
from requests.exceptions import Timeout
from requests.exceptions import ConnectionError
from requests.exceptions import TooManyRedirects
import json
import time
from portfolio_package import sqlite_modul as sqm
from portfolio_package import analyst_modul as anm
import configparser
import os
import logging
from time import sleep
from datetime import datetime, timedelta, date, time

name_program = 'yahoo_agent_info_2_rus'
comment_prog = 'получение сектора, индустрии и таргета с сайта finance.yahoo.com'

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
# задержка между запросами
frequency_req_serv_info = 5


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

    # программа используется только для базы rus, поэтому сразу выбираем нужный путь из config-файла
    path = config.get("Settings", "path_1")
    name_db = config.get("Settings", "name_db_1")
    name_main_table = config.get("Settings", "name_main_table")
    index_ticker = int(config.get("Settings", "index_ticker"))


def create_url_request(ticker: str, module_name: str):
    url = 'https://query1.finance.yahoo.com/v10/finance/quoteSummary/'
    url = url + ticker + '.ME' + '?modules='
    url = url + module_name
    return url


# отправка запроса и получение ответа
def requests_get(url: str, delay: int):
    name_func = 'requests_get: '
    # кол-во попыток на 1 запрос
    max_count_req = 3
    # минимальная задержка между запросами
    delay_ = delay
    # формирование url для запроса
    url_requests = url
    # HEADERS = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:71.0) Gecko/20100101 Firefox/71.0',
    #            'accept': '*/*'}
    print_comment = True

    try:
        response = requests.get(url_requests, headers=headers, params=None)
        if response.status_code == 200:
            return response
        else:
            if print_comment:
                print("Ошибка. Статус код ответа сервера: ", response.status_code,
                      ' url= ', url_requests)
            logging.warning("Ошибка. Статус код ответа сервера %d url= %s ", response.status_code, url_requests)
            # пробуем сделать еще count_req запросов
            i = 1
            for _ in range(max_count_req):
                # на каждом новом запросе будем увеличивать задержку
                time.sleep(delay_ * i)
                i = i + 1
                response = requests.get(url_requests, headers=headers, params=None)
                if response.status_code == 200:
                    return response
                else:
                    count = i - 1
                    if print_comment:
                        print(count, " ", "Ошибка. Статус код ответа с сервера: ", response.status_code, " ждем ",
                              count * delay_, " сек")
                    logging.warning('%d. Ошибка. Статус код ответа с сервера %d, ждем %d секунд',
                                    count, response.status_code, count * delay_)

            # если дошли сюда, ответ 200 так и не был получен
            return False
    except InvalidURL:
        print("Неправильный URL: ", url_requests, " не отвечает. InvalidURL")
        logging.warning("Неправильный URL: %s не отвечает. InvalidURL", url_requests)
        return False
    except Timeout:
        print("Сервер по адресу: ", url_requests, " не отвечает. TimeoutError")
        logging.warning("Сервер по адресу: %s не отвечает. TimeoutError", url_requests)
        return False
    except ConnectionError:
        print("Сервер по адресу: ", url_requests,
              " не отвечает, ошибка в имени. ConnectionError")
        logging.warning("Сервер по адресу: %s не отвечает, ошибка в имени. ConnectionError", url_requests)
        return False
    except HTTPError:
        print("Сервер по адресу: ", url_requests, " не отвечает. HTTPError")
        logging.warning("Сервер по адресу: %s не отвечает. HTTPError", url_requests)
        return False
    except TooManyRedirects:
        print("Сервер по адресу: ", url_requests, " не отвечает. TooManyRedirects")
        logging.warning("Сервер по адресу: %s не отвечает. TooManyRedirects", url_requests)
        return False
    except:
        print("Сервер по адресу: ", url_requests, " не отвечает. Unknown error")
        logging.warning("Сервер по адресу: %s не отвечает. Unknown error", url_requests)
        return False


def profile_modul(conn_obj, curs_obj, ticker: str, module_name: str):
    # задержка между запросами
    delay = 30
    # формируем запрос
    r = requests_get(create_url_request(ticker, module_name), delay)
    if r is not False:
        # получен ответ сервера
        result = r.json()['quoteSummary']['result'][0]
        sector = result['assetProfile']['sector']
        industry = result['assetProfile']['industry']
        # вставляем значения в таблицу
        sql_obj.update_date_in_cell(conn_obj, curs_obj, name_main_table,
                                    'Sector', sector, 'Ticker',
                                    ticker)
        sql_obj.update_date_in_cell(conn_obj, curs_obj, name_main_table,
                                    'Industry', industry, 'Ticker',
                                    ticker)
    else:
        print("Не удалось обновить данные по сектору или индустрии для тикера ", ticker)
        logging.warning("Не удалось обновить данные по сектору или индустрии для тикера %s", ticker)


def default_key_statistics_modul(conn_obj, curs_obj, ticker: str, module_name: str):
    # задержка между запросами
    delay = 30
    # формируем запрос
    r = requests_get(create_url_request(ticker, module_name), delay)
    if r is not False:
        market_cap = 0
        market_cap_str = 'No_data'
        p_e = 0
        margin = 0
        beta = 0

        # получен ответ сервера
        result = r.json()['quoteSummary']['result'][0]
        if 'raw' in result['defaultKeyStatistics']['enterpriseValue']:
            market_cap_ = result['defaultKeyStatistics']['enterpriseValue']['raw']
            if type(market_cap_) != str:
                # print(ticker, ' market_cap_ ', market_cap_, ' type(market_cap_) ', type(market_cap_))
                market_cap = abs(result['defaultKeyStatistics']['enterpriseValue']['raw'])

        if market_cap > 0:
            # градация российский рынок
            # 'Small' до 1 млрд. дол.
            # 'Middle' 1 - 10 млрд. дол.
            # 'Big' > 10 млрд. дол.
            # данные с Yahoo похоже в миллиардах рублей
            # получим курс ЦБ
            date_current = datetime.now()
            exchange_rate = sql_obj.get_exchange_rates(date_current.day, date_current.month, date_current.year)
            market_cap = market_cap / float(exchange_rate['dollar'])
            if market_cap > 10000000000:
                market_cap_str = 'Big'
            elif (market_cap < 10000000000) and (market_cap > 1000000000):
                market_cap_str = 'Middle'
            else:
                market_cap_str = 'Small'

        if 'raw' in result['defaultKeyStatistics']['forwardPE']:
            p_e_ = result['defaultKeyStatistics']['forwardPE']['raw']
            if type(p_e_) != str:
                # print(ticker, ' p_e_ ', p_e_, ' type(p_e_) ', type(p_e_))
                p_e = round(result['defaultKeyStatistics']['forwardPE']['raw'], 2)

        if 'raw' in result['defaultKeyStatistics']['profitMargins']:
            margin_ = result['defaultKeyStatistics']['profitMargins']['raw']
            if type(margin_) != str:
                # print(ticker, ' margin_ ', margin_, ' type(margin_) ', type(margin_))
                margin = round(result['defaultKeyStatistics']['profitMargins']['raw'] * 100.0, 2)

        if 'raw' in result['defaultKeyStatistics']['beta']:
            beta_ = result['defaultKeyStatistics']['beta']['raw']
            if type(beta_) != str:
                # print(ticker, ' beta_ ', beta_, ' type(beta_) ', type(beta_))
                beta = round(result['defaultKeyStatistics']['beta']['raw'], 2)

        # вставляем значения в таблицу в миллиардах долларов
        sql_obj.update_date_in_cell(conn_obj, curs_obj, name_main_table,
                                    'Market_Cap', market_cap_str, 'Ticker',
                                    ticker)
        sql_obj.update_date_in_cell(conn_obj, curs_obj, name_main_table,
                                    'MCap', round((market_cap / 1000000000), 4), 'Ticker',
                                    ticker)
        sql_obj.update_date_in_cell(conn_obj, curs_obj, name_main_table,
                                    'PE', p_e, 'Ticker',
                                    ticker)
        sql_obj.update_date_in_cell(conn_obj, curs_obj, name_main_table,
                                    'Margin', margin, 'Ticker',
                                    ticker)
        sql_obj.update_date_in_cell(conn_obj, curs_obj, name_main_table,
                                    'Beta', beta, 'Ticker',
                                    ticker)
    else:
        print("Не удалось обновить данные по ключевой статистике для тикера ", ticker)
        logging.warning("Не удалось обновить данные по ключевой статистике для тикера %s", ticker)


def recommendation_trend_modul(conn_obj, curs_obj, ticker: str, module_name: str):
    # задержка между запросами
    delay = 30
    # формируем запрос
    r = requests_get(create_url_request(ticker, module_name), delay)
    if r is not False:
        # получен ответ сервера
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
            delta = timedelta(days=30)

            # начинаем с самых дальних рекоммендаций -3m
            result = r.json()['quoteSummary']['result'][0]
            recom = result['recommendationTrend']['trend'][3]
            # {'period': '-3m', 'strongBuy': 6, 'buy': 13, 'hold': 5, 'sell': 1, 'strongSell': 0}
            # вычиcляем рейтинг и месяц рейтинга
            raiting_3m = raiting_calculation(recom)
            d = current_date - delta * 3
            raiting_date = datetime(d.year, d.month, 1, 0, 0, 0)
            r_time = time(0, 0)
            # если рейтинг > 0 и нет строки с соответствующей датой, добавляем ее
            if raiting_3m > 0 and sql_obj.select_date_time_(curs_obj, name_table_recommendation, 'Period', raiting_date,
                                                         r_time) is None:
                # print('Строки с указанной датой еще нет')
                data_load = (ticker,
                             sql_obj.current_date(),
                             raiting_date,
                             recom['strongBuy'],
                             recom['buy'],
                             recom['hold'],
                             recom['sell'],
                             recom['strongSell'],
                             raiting_3m,
                             0,
                             ''
                             )
                q = "INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                q_ = q.format(table=name_table_recommendation)
                curs_obj.execute(q_, data_load)
                conn_obj.commit()
            else:
                # print('Строка с указанной датой уже есть')
                pass

            # рекоммендации -2m
            result = r.json()['quoteSummary']['result'][0]
            recom = result['recommendationTrend']['trend'][2]
            # {'period': '-2m', 'strongBuy': 6, 'buy': 13, 'hold': 5, 'sell': 1, 'strongSell': 0}
            # вычиcляем рейтинг и месяц рейтинга
            raiting_2m = raiting_calculation(recom)
            d = current_date - delta * 2
            raiting_date = datetime(d.year, d.month, 1, 0, 0, 0)
            r_time = time(0, 0)
            # если нет строки с соответствующей датой, добавляем ее
            if raiting_2m > 0 and sql_obj.select_date_time_(curs_obj, name_table_recommendation, 'Period', raiting_date,
                                                         r_time) is None:
                # print('Строки с указанной датой еще нет')
                data_load = (ticker,
                             sql_obj.current_date(),
                             raiting_date,
                             recom['strongBuy'],
                             recom['buy'],
                             recom['hold'],
                             recom['sell'],
                             recom['strongSell'],
                             raiting_2m,
                             0,
                             ''
                             )
                q = "INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                q_ = q.format(table=name_table_recommendation)
                curs_obj.execute(q_, data_load)
                conn_obj.commit()
            else:
                # print('Строка с указанной датой уже есть')
                pass

            # рекоммендации -1m
            result = r.json()['quoteSummary']['result'][0]
            recom = result['recommendationTrend']['trend'][1]
            # {'period': '-1m', 'strongBuy': 6, 'buy': 13, 'hold': 5, 'sell': 1, 'strongSell': 0}
            # вычиcляем рейтинг и месяц рейтинга
            raiting_1m = raiting_calculation(recom)
            d = current_date - delta * 1
            raiting_date = datetime(d.year, d.month, 1, 0, 0, 0)
            r_time = time(0, 0)
            # если нет строки с соответствующей датой, добавляем ее
            if raiting_1m > 0 and sql_obj.select_date_time_(curs_obj, name_table_recommendation, 'Period', raiting_date,
                                                         r_time) is None:
                # print('Строки с указанной датой еще нет')
                data_load = (ticker,
                             sql_obj.current_date(),
                             raiting_date,
                             recom['strongBuy'],
                             recom['buy'],
                             recom['hold'],
                             recom['sell'],
                             recom['strongSell'],
                             raiting_1m,
                             0,
                             ''
                             )
                q = "INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                q_ = q.format(table=name_table_recommendation)
                curs_obj.execute(q_, data_load)
                conn_obj.commit()
            else:
                # print('Строка с указанной датой уже есть')
                pass

            # рекоммендации текущий месяц
            result = r.json()['quoteSummary']['result'][0]
            recom = result['recommendationTrend']['trend'][0]
            # {'period': '-0m', 'strongBuy': 6, 'buy': 13, 'hold': 5, 'sell': 1, 'strongSell': 0}
            # вычиcляем рейтинг и месяц рейтинга
            raiting_0m = raiting_calculation(recom)
            d = current_date
            raiting_date = datetime(d.year, d.month, 1, 0, 0, 0)
            r_time = time(0, 0)
            # если нет строки с соответствующей датой, добавляем ее
            if sql_obj.select_date_time_(curs_obj, name_table_recommendation, 'Period', raiting_date, r_time) is None:
                # print('Строки с указанной датой еще нет')
                data_load = (ticker,
                             sql_obj.current_date(),
                             raiting_date,
                             recom['strongBuy'],
                             recom['buy'],
                             recom['hold'],
                             recom['sell'],
                             recom['strongSell'],
                             raiting_0m,
                             0,
                             ''
                             )
                q = "INSERT INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                q_ = q.format(table=name_table_recommendation)
                curs_obj.execute(q_, data_load)
                conn_obj.commit()
            else:
                # print('Строка с указанной датой уже есть')
                pass

            # в сводную таблицу записывается рейтинг -1m как на сайте yahoo
            # если рейтинг = 0, выбираем из предыдущих или текущий если их нет
            raiting = raiting_1m
            if raiting == 0:
                raiting = raiting_2m
            if raiting == 0:
                raiting = raiting_3m
            if raiting == 0:
                raiting = raiting_0m
            if sql_obj.check_table_is_exists(curs_obj, name_main_table):
                sql_obj.update_date_in_cell(conn_obj, curs_obj, name_main_table,
                                            'Date_r_avto', raiting_date, 'Ticker',
                                            ticker)
                sql_obj.update_data_in_cell(conn_obj, curs_obj, name_main_table,
                                            'Recom_avto', str(raiting), 'Ticker',
                                            ticker)

            # следующий шаг прописать изменение рейтинга
            # загружаем столбцы с итоговой рекоммендацией и датой и идем по ним снизу вверх
            q = '''SELECT {column}, {column_1} FROM {table}'''
            q_ = q.format(table=name_table_recommendation, column='Sum_recom', column_1='Period')
            curs_obj.execute(q_)
            list_recom = curs_obj.fetchall()
            # [(2.0, '2021-06-01 00:00:00'), (2.1, '2021-07-01 00:00:00'), (2.1, '2021-08-01 00:00:00'),
            # (2.2, '2021-09-01 00:00:00')]
            # запускаем цикл справа налево
            size_frame = len(list_recom)
            if size_frame > 0:
                for i in range(size_frame-1, 0, -1):
                    # если предыдущее значение существует
                    if (i-1) >= 0:
                        # изменение рейтинга
                        if list_recom[i][0] > 0 and list_recom[i-1][0] > 0:
                            change_raiting = list_recom[i][0] - list_recom[i-1][0]
                            # если разность > 0 то это понижение в терминах Yahoo
                            change_raiting_str = ''
                            if change_raiting < 0:
                                change_raiting_str = 'Up'
                            if change_raiting > 0:
                                change_raiting_str = 'Down'
                            # заносим данные в таблицу
                            sql_obj.update_data_in_cell(conn_obj, curs_obj, name_table_recommendation,
                                                        'Change', str(change_raiting), 'Period',
                                                        list_recom[i][1])
                            sql_obj.update_date_in_cell(conn_obj, curs_obj, name_table_recommendation,
                                                        'Change_str', change_raiting_str, 'Period',
                                                        list_recom[i][1])


def raiting_calculation(raiting_list: dict):
    # веса
    strong_buy = 1
    buy = 2
    hold = 3
    sell = 4
    strong_sell = 5

    if raiting_list is None:
        return 0

    summ = raiting_list['strongBuy'] * strong_buy
    summ = summ + raiting_list['buy'] * buy
    summ = summ + raiting_list['hold'] * hold
    summ = summ + raiting_list['sell'] * sell
    summ = summ + raiting_list['strongSell'] * strong_sell
    count = raiting_list['strongBuy'] + raiting_list['buy'] + raiting_list['hold'] + raiting_list['sell'] + \
            raiting_list['strongSell']
    # print('count ', count, ' summ ', summ)

    if count == 0:
        return 0
    else:
        return round(summ/count, 1)


def target_trend_modul(conn_obj, curs_obj, ticker: str, module_name: str):
    # задержка между запросами
    delay = 30
    # формируем запрос
    r = requests_get(create_url_request(ticker, module_name), delay)
    if r is not False:
        # получен ответ сервера
        result = r.json()['quoteSummary']['result'][0]
        mean_price = result['financialData']['targetMeanPrice']
        target_0m = 0
        if mean_price is None or len(mean_price) == 0:
            return
        else:
            target_0m = float(mean_price['raw'])

        # проверим существование таблицы
        name_table_target = ticker + '_Target_trend'
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
            target_date = datetime(current_date.year, current_date.month, 1, 0, 0, 0)
            r_time = time(0, 0)

            if target_0m > 0:
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
logging.basicConfig(filename='yahoo_agent_info_2_rus.log',
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
                # Industry, Date_r_avto, Recom_avto, Lot, Listing

                # если поле Sector или Industry пустое, заполняем его
                if row[5] == '' or row[6] == '':
                    profile_modul(con, cursorObj, current_ticker, 'assetProfile')

                # загрузка ключевой статистики
                default_key_statistics_modul(con, cursorObj, current_ticker, 'defaultKeyStatistics')

                # загрузка текущих рекоммендаций аналитиков
                # recommendation_trend_modul(con, cursorObj, current_ticker, 'recommendationTrend')

                # загрузка средней цены как таргета аналитиков
                target_trend_modul(con, cursorObj, current_ticker, 'financialData')

                # включаем задержку перед новым запросом на сервер
                print("Получили информацию по тикеру ", current_ticker)
                logging.info('Получили информацию по тикеру %s', current_ticker)
                sleep(frequency_req_serv_info)
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


