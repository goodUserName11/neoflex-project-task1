__all__ = ['add_ballance', 'add_posting', 'add_account', 'add_currency', 'add_exchange_rate', 'add_ledger_account']

from airflow.providers.postgres.hooks.postgres import PostgresHook

from sqlalchemy import MetaData
from sqlalchemy import update
from sqlalchemy import and_

import pandas
from io import StringIO


# Функция получения из файла и добавление записей ft_balance_f
def add_ballance(table_name: str, encoding: str = 'utf_8', path: str = '/') -> None:
    # Чтение из файла
    with open(path + f'{table_name}.csv') as f:
        data = f.read()
    
    # Короткий путь, чтобы сменить регистр в написании заголовков столбцов (вместо df.rename() из pandas (но вероятно не такой быстрый)) 
    datas = data.splitlines()
    datas[0] = datas[0].lower()
    data = '\n'.join(datas)

    # Преобразование данных в DataFrame, приведение к нужным типам
    df = pandas.read_csv(StringIO(data), 
                        delimiter=';', 
                        encoding = encoding, 
                        parse_dates = [0], 
                        date_format = '%d.%m.%Y')
    # Преобразование pandas NA в None, чтобы записывать отсутсвующие значения в таблицу
    df = df.where(pandas.notnull(df), None)
    df = df.to_dict('records')
    
    # Подключение к базе данный и получение таблицы 
    postgres_hook = PostgresHook('postgres-db')
    engine = postgres_hook.get_sqlalchemy_engine()
    meta_data = MetaData(bind=engine, schema="ds")
    meta_data.reflect(engine)
    table = meta_data.tables['ds.' + table_name]
    
    with engine.begin() as connection:
        for item in df:
            # Обновление строки таблицы
            res = connection.execute(
                update(table)
                .where(and_(
                    table.c.on_date == item["on_date"], 
                    table.c.account_rk == item["account_rk"]))
                .values(item)
                .returning(table))
            
            # Если не было обновления, то вставка
            if(res.rowcount == 0):
                connection.execute(table.insert().values(item))

# Функция получения из файла и добавление записей ft_posting_f
def add_posting(table_name: str, encoding: str = 'utf_8', path: str = '/') -> None:
    # Чтение из файла
    with open(path + f'{table_name}.csv') as f:
        data = f.read()
    
    # Короткий путь, чтобы сменить регистр в написании заголовков столбцов (вместо df.rename() из pandas (но вероятно не такой быстрый)) 
    datas = data.splitlines()
    datas[0] = datas[0].lower()
    data = '\n'.join(datas)

    # Преобразование данных в DataFrame, приведение к нужным типам
    df = pandas.read_csv(StringIO(data), 
                        delimiter=';', 
                        encoding = encoding, 
                        parse_dates = [0], 
                        date_format = '%d-%m-%Y')
    # Преобразование pandas NA в None, чтобы записывать отсутсвующие значения в таблицу
    df = df.where(pandas.notnull(df), None)
    df = df.to_dict('records')
    
    # Подключение к базе данный и получение таблицы 
    postgres_hook = PostgresHook('postgres-db')
    engine = postgres_hook.get_sqlalchemy_engine()
    meta_data = MetaData(bind=engine, schema="ds")
    meta_data.reflect(engine)
    table = meta_data.tables['ds.' + table_name]

    with engine.begin() as connection:
        # Удаление значений из таблицы
        connection.execute(table.delete())

        # Вставка значений в таблицу
        for item in df:
            connection.execute(table.insert().values(item))

# Функция получения из файла и добавление записей md_account_d
def add_account(table_name: str, encoding: str = 'utf_8', path: str = '/') -> None:
    # Чтение из файла
    with open(path + f'{table_name}.csv') as f:
        data = f.read()
    
    # Короткий путь, чтобы сменить регистр в написании заголовков столбцов (вместо df.rename() из pandas (но вероятно не такой быстрый)) 
    datas = data.splitlines()
    datas[0] = datas[0].lower()
    data = '\n'.join(datas)

    # Преобразование данных в DataFrame, приведение к нужным типам
    df = pandas.read_csv(StringIO(data), 
                        delimiter=';', 
                        encoding = encoding, 
                        parse_dates = [0, 1], 
                        date_format = '%Y-%m-%d',
                        dtype = {'currency_code': str})
    # Преобразование pandas NA в None, чтобы записывать отсутсвующие значения в таблицу
    df = df.where(pandas.notnull(df), None)
    df = df.to_dict('records')
    
    # Подключение к базе данный и получение таблицы 
    postgres_hook = PostgresHook('postgres-db')
    engine = postgres_hook.get_sqlalchemy_engine()
    meta_data = MetaData(bind=engine, schema="ds")
    meta_data.reflect(engine)
    table = meta_data.tables['ds.' + table_name]
    
    with engine.begin() as connection:
        for item in df:
            # Обновление строки таблицы
            res = connection.execute(
                update(table)
                .where(and_(
                    table.c.data_actual_date == item["data_actual_date"], 
                    table.c.account_rk == item["account_rk"]))
                .values(item)
                .returning(table))
            
            # Если не было обновления, то вставка
            if(res.rowcount == 0):
                connection.execute(table.insert().values(item))

# Функция получения из файла и добавление записей md_currency_d
def add_currency(table_name: str, encoding: str = 'utf_8', path: str = '/') -> None:
    # Чтение из файла
    with open(path + f'{table_name}.csv', encoding = encoding) as f:
        data = f.read()
    
    # Короткий путь, чтобы сменить регистр в написании заголовков столбцов (вместо df.rename() из pandas (но вероятно не такой быстрый)) 
    datas = data.splitlines()
    datas[0] = datas[0].lower()
    data = '\n'.join(datas)

    # Преобразование данных в DataFrame, приведение к нужным типам
    df = pandas.read_csv(StringIO(data), 
                        delimiter=';', 
                        encoding = encoding, 
                        parse_dates = [1, 2], 
                        date_format = '%Y-%m-%d',
                        dtype={'currency_code': str, 'code_iso_char': str})
    # Преобразование pandas NA в None, чтобы записывать отсутсвующие значения в таблицу
    df = df.where(pandas.notnull(df), None)
    df = df.to_dict('records')
    
    # Подключение к базе данный и получение таблицы 
    postgres_hook = PostgresHook('postgres-db')
    engine = postgres_hook.get_sqlalchemy_engine()
    meta_data = MetaData(bind=engine, schema="ds")
    meta_data.reflect(engine)
    table = meta_data.tables['ds.' + table_name]
    
    with engine.begin() as connection:
        for item in df:
            # Обновление строки таблицы
            res = connection.execute(
                update(table)
                .where(and_(
                    table.c.currency_rk == item["currency_rk"], 
                    table.c.data_actual_date == item["data_actual_date"]))
                .values(item)
                .returning(table))
            
            # Если не было обновления, то вставка
            if(res.rowcount == 0):
                connection.execute(table.insert().values(item))

# Функция получения из файла и добавление записей md_exchange_rate_d
def add_exchange_rate(table_name: str, encoding: str = 'utf_8', path: str = '/') -> None:
    # Чтение из файла
    with open(path + f'{table_name}.csv', encoding = encoding) as f:
        data = f.read()
    
    # Короткий путь, чтобы сменить регистр в написании заголовков столбцов (вместо df.rename() из pandas (но вероятно не такой быстрый)) 
    datas = data.splitlines()
    datas[0] = datas[0].lower()
    data = '\n'.join(datas)

    # Преобразование данных в DataFrame, приведение к нужным типам
    df = pandas.read_csv(StringIO(data), 
                        delimiter=';', 
                        encoding = encoding, 
                        parse_dates = [0, 1], 
                        date_format = '%Y-%m-%d',
                        dtype={'code_iso_num': str})
    # Преобразование pandas NA в None, чтобы записывать отсутсвующие значения в таблицу
    df = df.where(pandas.notnull(df), None)
    print(df.dtypes)
    df = df.to_dict('records')
    
    # Подключение к базе данный и получение таблицы 
    postgres_hook = PostgresHook('postgres-db')
    engine = postgres_hook.get_sqlalchemy_engine()
    meta_data = MetaData(bind=engine, schema="ds")
    meta_data.reflect(engine)
    table = meta_data.tables['ds.' + table_name]
    
    with engine.begin() as connection:
        for item in df:
            # Обновление строки таблицы
            res = connection.execute(
                update(table)
                .where(and_(
                    table.c.data_actual_date == item["data_actual_date"], 
                    table.c.currency_rk == item["currency_rk"]))
                .values(item)
                .returning(table))
            
            # Если не было обновления, то вставка
            if(res.rowcount == 0):
                connection.execute(table.insert().values(item))

# Функция получения из файла и добавление записей md_ledger_account_s
def add_ledger_account(table_name: str, encoding: str = 'utf_8', path: str = '/') -> None:
    # Чтение из файла
    with open(path + f'{table_name}.csv', encoding = encoding) as f:
        data = f.read()
    
    # Короткий путь, чтобы сменить регистр в написании заголовков столбцов (вместо df.rename() из pandas (но вероятно не такой быстрый)) 
    datas = data.splitlines()
    datas[0] = datas[0].lower()
    data = '\n'.join(datas)

    # Преобразование данных в DataFrame, приведение к нужным типам
    df = pandas.read_csv(StringIO(data), 
                        delimiter=';', 
                        encoding = encoding, 
                        parse_dates = [10, 11], 
                        date_format = '%Y-%m-%d')
    # Преобразование pandas NA в None, чтобы записывать отсутсвующие значения в таблицу
    df = df.where(pandas.notnull(df), None)
    print(df.dtypes)
    df = df.to_dict('records')
    
    # Подключение к базе данный и получение таблицы 
    postgres_hook = PostgresHook('postgres-db')
    engine = postgres_hook.get_sqlalchemy_engine()
    meta_data = MetaData(bind=engine, schema="ds")
    meta_data.reflect(engine)
    table = meta_data.tables['ds.' + table_name]
    
    with engine.begin() as connection:
        for item in df:
            # Обновление строки таблицы
            res = connection.execute(
                update(table)
                .where(and_(
                    table.c.ledger_account == item["ledger_account"], 
                    table.c.start_date == item["start_date"]))
                .values(item)
                .returning(table))
            
            # Если не было обновления, то вставка
            if(res.rowcount == 0):
                connection.execute(table.insert().values(item))