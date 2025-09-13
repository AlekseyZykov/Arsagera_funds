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
    cursor.execute("""CREATE SCHEMA IF NOT exists arsagera;""")
    connection.commit()
    cursor.close()
    connection.close()

    # Подключаемся к базе через sqlalchemy
    url = f'postgresql://{cred['user']}:{cred['password']}@{cred['host']}:{cred['port']}/{cred['dbname']}'
    engine = create_engine(url)

    # Фонды Арсагеры
    fund_code_dict = {
        'fund_id': [1, 2, 3, 4],
        'ticket': ['fa', 'f4si', 'f64', 'fo'],
        'fund_name': ["Арсагера — фонд акций", "Арсагера — фонд смешанных инвестиций",
                      "Арсагера — акции 6.4", "Арсагера – фонд облигаций КР 1.55"]
    }

    # Считываем словарь фондов в DataFrame
    df = pd.DataFrame(fund_code_dict)
    # Записываем в таблицу справочник фондов fund_dict
    df.to_sql('fund_dict', con=engine, schema='arsagera',
              if_exists='replace', index=False)

    # В цикле скачиваем данные всех фондов
    for key in fund_code_dict['ticket']:
        # Делаем get-запрос
        response = requests.get(
            f'https://arsagera.ru/api/v1/funds/{key}/fund-metrics/').json()['data']
        response = str(response)
        # Записываем полученные данные в json файл
        with open(f'{key}' + '.json', 'w', encoding='utf-8') as f:
            # Заменяем одиночные ковычки на двойные для чтения pandas
            f.write(response.replace('\'', '\"'))
        df = pd.read_json(f'{key}' + '.json')
        # Записываем данные в stg-таблицы БД postgres
        df.to_sql('stg_' + f'{key}', con=engine, schema='arsagera',
                  if_exists='replace', index=False)
        # Удаляем временные файлы json
        os.remove(f'{key}' + '.json')
