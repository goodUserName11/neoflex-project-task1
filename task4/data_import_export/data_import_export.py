__all__ = ['export_to_csv', 'import_from_csv']

import pandas
from sqlalchemy import create_engine, MetaData

from logger_setup import setup_logger

logger = setup_logger()

# Функция выгрузки данных из таблицы базы данных в csv-файл
def export_to_csv(db_name, user, password, host, port, schema, table_name, output_file):
    logger.info(f"Start of export to csv from table '{table_name}'")

    # Создаем строку подключения
    connection_string = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}'
     # Создаем движок SQLAlchemy
    engine = create_engine(connection_string)

    try:
        # Используем pandas для чтения данных из базы данных в DataFrame
        df = pandas.read_sql_table(table_name, schema=schema, con=engine)
        
        # Экспортируем DataFrame в CSV файл
        df.to_csv(output_file, header=True, index=False)
        
        logger.info(f"Data from table '{table_name}' successfully exported to '{output_file}'.")
    
    except Exception as e:
        logger.info(f"Error of data export: {e}")
        raise
    
    finally:
        # Закрываем соединение
        engine.dispose()

# Функция импрорта данных из csv-файла в таблицу базы данных
def import_from_csv(db_name, user, password, host, port, schema, table_name, input_file, date_format = None):
    logger.info(f"Start of import from csv '{input_file}'")

    # Создаем строку подключения
    connection_string = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}'
     # Создаем движок SQLAlchemy
    engine = create_engine(connection_string)

    try:
        # Используем pandas для чтения данных из файла в DataFrame
        df = pandas.read_csv(
            input_file, 
            header=0, 
            date_format=date_format)
        # Сохранение в базу данных
        df.to_sql(
            table_name, 
            engine, 
            schema=schema, 
            if_exists='append', 
            index=False)

        logger.info(f"Data from table '{input_file}' successfully imported to table '{table_name}'.")
    
    except Exception as e:
        logger.info(f"Error of data export: {3}")
        raise
    
    finally:
        # Закрываем соединение
        engine.dispose()
