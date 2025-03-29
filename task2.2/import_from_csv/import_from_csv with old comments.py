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

# Python отфильтровать список из словарей, чтобы все значения одного из столбцов были внутри другого списка
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

            # print(len(filtered_df))

            # Вставляем в таблицу новые данные
            connection.execute(table.insert().values(filtered_df))

            # for item in filtered_df:
            #     print(item)
            #     # for row in res:
            #     #     print(row[0], type(row[0]))
                
            #     # print(item['effective_from_date'], type(item['effective_from_date'].to_pydatetime().date()))
                
            #     # date_to_check = datetime.strptime('2023-03-15', '%Y-%m-%d').date()
            #     # # print(date_to_check)

            #     # if item['effective_from_date'].to_pydatetime().date() == date_to_check:
            #     #     print('ok5')
            #     #     # break

            #     if item['effective_from_date'].to_pydatetime().date() not in existing_list:
            #         # print('ok4')
            #         connection.execute(table.insert().values(item))
    finally:
        # Закрываем соединение
        engine.dispose()