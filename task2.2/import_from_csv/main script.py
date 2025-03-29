from import_from_csv import *

if __name__ == "__main__":
    # Добавление части данных в rd.deal_info
    # Так как в файле присуствуют данные, которые отсутствуют в таблице, полная перезагрузка не имеет смысла.
    # Поэтому была выбрана загрузка части данных 
    import_from_csv(
        db_name='dwh',
        user='postgresuser',
        password='123456',
        host='localhost',
        port='5432',
        schema='rd',
        table_name='deal_info',
        input_file='loan_holiday_info\deal_info.csv',
        date_format='%Y-%m-%d',
        parse_dates=[11,12],
        encoding='cp1251'
    )

    # В файле присутствуют данные за даты, которые уже есть в таблице базы данных.
    # Была выбрана частичная загрузка, чтобы не потерять уже имеющиеся данные и избежать возможных ошибок
    # Добавление части данных в rd.product
    import_from_csv_by_date(
        db_name='dwh',
        user='postgresuser',
        password='123456',
        host='localhost',
        port='5432',
        schema='rd',
        table_name='product',
        input_file='loan_holiday_info\product_info.csv',
        date_format='%Y-%m-%d',
        parse_dates=[2,3],
        encoding='cp1251'
    )