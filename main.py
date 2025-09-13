import py_scripts.from_arsagera_api_to_postgresql
import py_scripts.sql_transform


# Скачиваем данные по api и загружаем в postgresql
py_scripts.from_arsagera_api_to_postgresql.to_postgresql()


py_scripts.sql_transform.sql_transform()
