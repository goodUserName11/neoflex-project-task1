__all__ = ['insert_log_entry', 'end_log_entry']

from airflow.providers.postgres.hooks.postgres import PostgresHook

from sqlalchemy import MetaData

from datetime import datetime

# Добавить новую строку в лог
def insert_log_entry(table_name: str, log_level: str, message: str, **kwargs):
    insert_values = {'log_level': log_level, 'log_message': message}

    postgres_hook = PostgresHook('postgres-db')
    engine = postgres_hook.get_sqlalchemy_engine()
    meta_data = MetaData(bind=engine, schema="logs")
    meta_data.reflect(engine)
    table = meta_data.tables['logs.' + table_name]

    with engine.begin() as connection:
        res = connection.execute(
            table.insert()
            .values(insert_values)
            .returning(table))

    # добавление id записи в контекст
    kwargs['ti'].xcom_push(key='end_log_id', value=res.all()[0][0])    

# Запись времени конца операции
def end_log_entry(table_name: str, **kwargs):
    # чтение id записи из контекста
    end_log_id = kwargs['ti'].xcom_pull(task_ids='start', key='end_log_id')
    end_log_id = int(end_log_id)

    print(end_log_id)

    postgres_hook = PostgresHook('postgres-db')
    engine = postgres_hook.get_sqlalchemy_engine()
    meta_data = MetaData(bind=engine, schema="logs")
    meta_data.reflect(engine)
    table = meta_data.tables['logs.' + table_name]

    with engine.begin() as connection:
        connection.execute(
            table.update()
            .values({'end_timestamp': datetime.now()})
            .where(table.c.log_id == end_log_id))