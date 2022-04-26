<h1 align="center">VIXAL - экспертная система для эффективного управления портфелем ценных бумаг</h1>

<p>Ядро системы - БД SQLite, в которую из различных источников собирается финансовая информация.</p>
<p>На основе собранных данных формируются аналитические отчеты, которые отправляются на
email. Для действующего портфеля выдаются рекомендации по ребалансировке.
Кроме того, есть инструмент для формирования оптимального портфеля из отобранных тикеров и
проверка его поведения на исторических данных.</p>

<img src="https://github.com/Lug77/VIXAL/blob/master/images/Vixal_block_chema_1.png" alt="Экспертная система VIXAL">

<h3>Подготовка БД</h3>

<p>Перед началом работы нужно создать БД. Например C:\DB_TEST\usa_market_test.db
Для иностранных бумаг и акций РФ лучше создать отдельные БД. Это связано с разными
источниками получения финансовой информации.</p>

<p>В БД нужно создать таблицу _Tickers, для получения котировок акций:</p>

<code>
  CREATE TABLE _Tickers
  	(
  		Ticker_ID integer,
  		Ticker text NOT NULL,
  		Ticker_name text DEFAULT '',
  		ISIN_code text DEFAULT '',
  		Exchange text DEFAULT '',
  		Sector text DEFAULT '',
  		Industry text DEFAULT '',
  		Date_r_avto date DEFAULT '1970.01.01',
  		Recom_avto real DEFAULT 0.0,
  		Lot real DEFAULT 1.0,
  		Listing text DEFAULT 'Yes',
  		Target real DEFAULT 0.0,
  		Market_Cap text DEFAULT '',
  		PE real DEFAULT 0.0,
  		Margin real DEFAULT 0.0,
  		Beta real DEFAULT 0.0,
  		MCap real DEFAULT 0.0
  )
</code>

<p>Колонку Ticker нужно заполнить кодами тикеров для отслеживания. Например, состав индекса S&P 100.
Если у вас есть действующий портфель ЦБ, то тикеры из него так же нужно добавить в _Tickers.</p>

<p>Далее создаем таблицу _Market, для получения котировок индексов, которые понадобятся на этапе тестирования
оптимального портфеля.</p>

<code>
  CREATE TABLE _Market
  	(
  		Ticker_ID integer PRIMARY KEY,
  		Ticker text DEFAULT '',
  		Ticker_name text DEFAULT '',
  		Sector text DEFAULT '',
  		Industry text DEFAULT '',
  		Listing text DEFAULT 'Yes',
  		Ticker_DB text DEFAULT ''
  )
</code>

<p>Добавление тикера S&P500:</p>

<code>INSERT INTO _Market
VALUES (1, '^GSPC', 'S&P500', 'All', 'All', 'Yes', 'GSPC')</code>

<p>И еще одну таблицу для хранения системных переменных</p>

<code>CREATE TABLE _System_variables
	(
		Ticker_ID integer PRIMARY KEY,
		Date_control_var date DEFAULT '1970.01.01',
		Name_var text DEFAULT '',
		Comment_var text DEFAULT '',
		Value_date date DEFAULT '1970.01.01',
		Value_text text DEFAULT '',
		Value_real real DEFAULT 0.0
)</code>

<p>Можно ее сразу загрузить из файла _System_variables__202204241032.csv</p>

<p>Основные параметры системы находятся в файле settings_expert_system.ini.
Проверьте названия и пути БД, прежде чем переходить к этапу загрузки.</p>
<p>Если скрипт запускается из IDE, файл должен находиться в том же каталоге, если запуск через планировщик
Windows - в C:\Windows\System32</p>

<h3>Загрузка финансовых данных</h3>

<p>Теперь можно приступать к загрузке данных. Скрипты можно запускать из IDE или сделать исполняемые файлы и использовать
планировщик Windows для запуска по расписанию.
Если используется один скрипт для работы с БД иностранных компаний и БД с акциями РФ,
используется аргумент в командной строке. Например:</p>
<code>--dir_db=usa</code><code>--dir_db=rus</code>

<ul>
  <li>Иностранные акции
    <p></p>
    <table>
      <tr>
        <td>Скрипт</td>
        <td>yahoo_agent_info.py</td>
      </tr>
      <tr>
        <td>Назначение</td>
        <td>загрузка с yahoo данных по иностранным акциям:
          <ul>
            <li>дневные и недельные котировки за 2 года с формированием таблицы</li>
            <li>информация по дивидендам с формированием таблицы</li>
            <li>история рекомендаций аналитиков с формированием таблицы</li>
              для всех тикеров из _Tickers
          </ul>
        </td>
      </tr>
      <tr>
        <td>Аргумент в командной строке</td>
        <td>нет</td>
      </tr>
      <tr>
        <td>Конфигурационный файл</td>
        <td>settings_expert_system.ini</td>
      </tr>
      <tr>
        <td>Запуск</td>
        <td>через Планировщик Windows, например 1 раз в неделю</td>
      </tr>
    </table>
    <p></p>    
    <p></p>
    <table>
      <tr>
        <td>Скрипт</td>
        <td>yahoo_agent_info_2.py</td>
      </tr>
      <tr>
        <td>Назначение</td>
        <td>загрузка с yahoo данных по иностранным акциям:
          <ul>
            <li>Сектор и индустрия</li>
            <li>PE, Margin, Beta, Mcap</li>
            <li>Текущие рекомендации аналитиков</li>
            <li>Таргеты</li>
              для всех тикеров из _Tickers
          </ul>
        </td>
      </tr>
      <tr>
        <td>Аргумент в командной строке</td>
        <td>нет</td>
      </tr>
      <tr>
        <td>Конфигурационный файл</td>
        <td>settings_expert_system.ini</td>
      </tr>
      <tr>
        <td>Запуск</td>
        <td>через Планировщик Windows, например 1 раз в неделю</td>
      </tr>
    </table>
    <p></p>
    <p></p>
    <table>
      <tr>
        <td>Скрипт</td>
        <td>parser_bcs_targets_usa.py</td>
      </tr>
      <tr>
        <td>Назначение</td>
        <td>парсинг сайта БКС и получение данных по иностранным акциям:
          <ul>
            <li>Таргеты</li>
              для всех тикеров из _Tickers
          </ul>
        </td>
      </tr>
      <tr>
        <td>Аргумент в командной строке</td>
        <td>нет</td>
      </tr>
      <tr>
        <td>Конфигурационный файл</td>
        <td>settings_expert_system.ini</td>
      </tr>
      <tr>
        <td>Запуск</td>
        <td>через Планировщик Windows, например 1 раз в неделю</td>
      </tr>
    </table>
    <p></p>            
  </li>
<li>Акции РФ
    <p></p>
    <table>
      <tr>
        <td>Скрипт</td>
        <td>yahoo_agent_info_2_rus.py</td>
      </tr>
      <tr>
        <td>Назначение</td>
        <td>загрузка с yahoo данных по акциям РФ:
          <ul>
            <li>Сектор и индустрия</li>
            <li>PE, Margin, Beta, Mcap</li>
            <li>таргеты</li>
              для всех тикеров из _Tickers
          </ul>
        </td>
      </tr>
      <tr>
        <td>Аргумент в командной строке</td>
        <td>нет</td>
      </tr>
      <tr>
        <td>Конфигурационный файл</td>
        <td>settings_expert_system.ini</td>
      </tr>
      <tr>
        <td>Запуск</td>
        <td>через Планировщик Windows, например 1 раз в неделю</td>
      </tr>
    </table>
    <p></p>
    <p></p>
    <table>
      <tr>
        <td>Скрипт</td>
        <td>parser_bcs.py</td>
      </tr>
      <tr>
        <td>Назначение</td>
        <td>парсинг сайта БКС и получение данных по акциям РФ:
          <ul>
            <li>информация по дивидендам с формированием таблицы</li>
            <li>полное имя тикера и isin-код</li>
            <li>сектор тикера</li>
              для всех тикеров из _Tickers
          </ul>
        </td>
      </tr>
      <tr>
        <td>Аргумент в командной строке</td>
        <td>нет</td>
      </tr>
      <tr>
        <td>Конфигурационный файл</td>
        <td>settings_expert_system.ini</td>
      </tr>
      <tr>
        <td>Запуск</td>
        <td>через Планировщик Windows, например 1 раз в неделю</td>
      </tr>
    </table>
    <p></p>
    <p></p>
    <table>
      <tr>
        <td>Скрипт</td>
        <td>parser_bcs_targets.py</td>
      </tr>
      <tr>
        <td>Назначение</td>
        <td>парсинг сайта БКС и получение данных по акциям РФ:
          <ul>
            <li>рекомендации аналитиков</li>
            <li>таргеты</li>
              для всех тикеров из _Tickers
          </ul>
        </td>
      </tr>
      <tr>
        <td>Аргумент в командной строке</td>
        <td>нет</td>
      </tr>
      <tr>
        <td>Конфигурационный файл</td>
        <td>settings_expert_system.ini</td>
      </tr>
      <tr>
        <td>Запуск</td>
        <td>через Планировщик Windows, например 1 раз в неделю</td>
      </tr>
    </table>
    <p></p>
    <p></p>
    <table>
      <tr>
        <td>Скрипт</td>
        <td>parser_arsagera_potencial.py</td>
      </tr>
      <tr>
        <td>Назначение</td>
        <td>парсинг сайта Арсагера и получение данных по акциям РФ:
          <ul>
            <li>получение потенциальных доходностей и вычисление таргетов</li>
              для всех тикеров из _Tickers
          </ul>
        </td>
      </tr>
      <tr>
        <td>Аргумент в командной строке</td>
        <td>нет</td>
      </tr>
      <tr>
        <td>Конфигурационный файл</td>
        <td>settings_expert_system.ini</td>
      </tr>
      <tr>
        <td>Запуск</td>
        <td>через Планировщик Windows, например 1 раз в неделю</td>
      </tr>
    </table>
    <p></p>
  </li>	
</ul>
