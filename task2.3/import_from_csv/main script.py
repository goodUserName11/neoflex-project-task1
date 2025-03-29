from import_from_csv import *

if __name__ == "__main__":
    # Выгрузка данных в базу данных из csv-файла для таблицы dm.dict_currency
    import_from_csv(
        db_name='dwh',
        user='postgresuser',
        password='123456',
        host='localhost',
        port='5432',
        schema='dm',
        table_name='dict_currency',
        input_file='dict_currency\dict_currency.csv',
        date_format='%Y-%m-%d',
        parse_dates=[2,3]
    )