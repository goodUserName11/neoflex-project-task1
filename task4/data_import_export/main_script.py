from data_import_export import *

if __name__ == "__main__":
    # Экспорт в csv
    export_to_csv(
        db_name='project_task1',
        user='postgresuser',
        password='123456',
        host='localhost',
        port='5432',
        schema='dm',
        table_name='dm_f101_round_f',
        output_file='output.csv'
    )

    # Импорт из csv
    import_from_csv(
        db_name='project_task1',
        user='postgresuser',
        password='123456',
        host='localhost',
        port='5432',
        schema='dm',
        table_name='dm_f101_round_f_v2',
        input_file='output.csv',
        date_format='%Y-%m-%d'
    )