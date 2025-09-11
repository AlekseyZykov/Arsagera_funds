import os
import requests
import json
import psycopg2
from sqlalchemy import create_engine
import pandas as pd


def to_postgresql():
    """Эта функция считывает по api исторические данные фондов УК Арсагера
    и помещает их в таблицы БД postgres"""

    # Читаем файл cred, в котором хранятся данные для подключения к БД postgres
    with open('cred.json', 'r', encoding='utf-8') as f:
        cred = json.load(f)

    # Подключаемся к базе через psycopg2 и создаем схему arsagera
    connection = psycopg2.connect(**cred)
    cursor = connection.cursor()
    cursor.execute("""CREATE SCHEMA IF NOT exists arsagera""")
    connection.commit()
    cursor.close()
    connection.close()

    # Подключаемся к базе через sqlalchemy
    url = f'postgresql://{cred['user']}:{cred['password']}@{cred['host']}:{cred['port']}/{cred['dbname']}'
    engine = create_engine(url)

    # Фонды Арсагеры
    fund_code_dict = {
        'fa': "Арсагера — фонд акций",
        'f4si': "Арсагера — фонд смешанных инвестиций",
        'f64': "Арсагера — акции 6.4",
        'fo': "Арсагера – фонд облигаций КР 1.55"
    }

    # В цикле скачиваем данные всех фондов
    for key in fund_code_dict.keys():
        # Делаем get-запрос
        response = requests.get(
            f'https://arsagera.ru/api/v1/funds/{key}/fund-metrics/').json()['data']
        response = str(response)
        # Записываем полученные данные в файл
        with open(f'{key}' + '.json', 'w', encoding='utf-8') as f:
            # Заменяем одиночные ковычки на двойные для чтения pandas
            f.write(response.replace('\'', '\"'))
        df = pd.read_json(f'{key}' + '.json')
        # Записываем данные в БД postgres
        df.to_sql('stg_' + f'{key}', con=engine, schema='arsagera',
                  if_exists='replace', index=False)
        # Удаляем временные файлы json
        os.remove(f'{key}' + '.json')
