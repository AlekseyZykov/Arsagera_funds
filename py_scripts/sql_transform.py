import json
import psycopg2


def sql_transform():
    """"Эта функция объединяет данные фондов в единую таблицу arsagera_funds"""
    # Читаем файл cred, в котором хранятся данные для подключения к БД postgres
    with open('cred.json', 'r', encoding='utf-8') as f:
        cred = json.load(f)

    # Подключаемся к базе через psycopg2 и создаем таблицу arsagera_funds
    connection = psycopg2.connect(**cred)
    cursor = connection.cursor()
    cursor.execute("""Set search_path to arsagera""")
    cursor.execute("""DROP TABLE IF exists arsagera_funds""")
    cursor.execute("""CREATE TABLE arsagera_funds
                (date date,
                nav_per_share numeric(8, 2),
                total_net_assets numeric(15, 2),
                fund_id integer);""")
    # Наполняем arsagera_funds данным из stg-таблиц
    cursor.execute("""INSERT INTO arsagera_funds(date, nav_per_share, total_net_assets, fund_id)
                   select date, nav_per_share, total_net_assets, 1
                   from stg_fa
                   union all
                   select date, nav_per_share, total_net_assets, 2
                   from stg_f4si
                   union all
                   select date, nav_per_share, total_net_assets, 3
                   from stg_f64
                   union all
                   select date, nav_per_share, total_net_assets, 4
                   from stg_fo""")
    connection.commit()
