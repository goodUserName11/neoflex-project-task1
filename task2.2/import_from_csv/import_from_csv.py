__all__ = ['import_from_csv', 'import_from_csv_by_date']

import pandas
from sqlalchemy import create_engine, MetaData, select
from datetime import date, datetime

# Функция импрорта данных из csv-файла в таблицу базы данных простой вставкой
def import_from_csv(
    db_name, 
    user, 
    password, 
    host, 
    port, 
    schema, 
    table_name, 
    input_file, 
    date_format=None,
    parse_dates=None, 
    encoding='utf-8'):
    # Создаем строку подключения
    connection_string = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}'
     # Создаем движок SQLAlchemy
    engine = create_engine(connection_string)

    try:
        # Используем pandas для чтения данных из файла в DataFrame
        df = pandas.read_csv(
            input_file, 
            header=0, 
            date_format=date_format,
            encoding=encoding)

        # Сохранение в базу данных
        df.to_sql(
            table_name, 
            engine, 
            schema=schema, 
            if_exists='append', 
            index=False)
    finally:
        # Закрываем соединение
        engine.dispose()

# Функция импорта данных из csv-файла в таблицу базы данных с помощью вставки только новых данных
def import_from_csv_by_date(
    db_name, 
    user, 
    password, 
    host, 
    port, 
    schema, 
    table_name, 
    input_file, 
    date_format=None,
    parse_dates=None,
    encoding='utf-8'):
    # Создаем строку подключения
    connection_string = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}'
     # Создаем движок SQLAlchemy
    engine = create_engine(connection_string)

    try:
        # Используем pandas для чтения данных из файла в DataFrame
        df = pandas.read_csv(
            input_file, 
            header=0, 
            date_format=date_format,
            parse_dates=[2,3],
            encoding=encoding)
        df = df.where(pandas.notnull(df), None)
        df = df.to_dict('records')

        meta_data = MetaData(schema=schema)
        meta_data.reflect(engine)
        
        # Получение таблицы
        table = meta_data.tables[schema + '.' + table_name]

        with engine.begin() as connection:
            res = connection.execute(
                    select(table.c.effective_from_date)
                    .group_by(table.c.effective_from_date)).fetchall()

            # Преобразуем каждую строку из таблицы в дату в списке
            existing_list = [row[0] for row in res] 
            # Убираем все уже имеющиеся даты
            filtered_df = [entry for entry in df if entry['effective_from_date'].to_pydatetime().date() not in existing_list]

            # Вставляем в таблицу новые данные
            connection.execute(table.insert().values(filtered_df))
    finally:
        # Закрываем соединение
        engine.dispose()