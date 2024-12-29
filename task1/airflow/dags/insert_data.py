from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

from datetime import datetime
import time

from data_load_funcs import *
from add_logs_funcs import *

# получение переменной с путем до файлов с данными
PATH = Variable.get('my_path')

# Обертка над функцией sleep для передачи в PythonOperator
def sleep(seconds: float = 5):
    # Функция задержки
    time.sleep(seconds)

# Аргументы для создания DAG
default_args = {
    'owner': 'kkireev',
    'start_date': datetime.now(),
    'retries': 0
}

# Декларация DAG
with DAG(
    'insert_data',
    default_args = default_args,
    description = 'Загрузка данных в схему ds',
    catchup = False,
    template_searchpath = [PATH],
    schedule = '0 0 * * *'
) as dag:

    start = PythonOperator(
        task_id = 'start',
        python_callable = insert_log_entry,
        op_kwargs = {'table_name': 'log_table', 'log_level': 'INFO', 'message': 'Processing data for ds shema'},
        provide_context = True
    )

    # Задача с задержкой на 5 секунд
    sleep_task = PythonOperator(
        task_id = 'sleep_task',
        python_callable = sleep,
        op_kwargs = {'seconds': 5}
    )

    # Добавление записей ft_balance_f
    add_balance_task = PythonOperator(
        task_id = 'add_balance_task',
        python_callable = add_ballance,
        op_kwargs = {'table_name': 'ft_balance_f', 'path': PATH}
    )
    
    # Добавление записей ft_posting_f
    add_posting_task = PythonOperator(
        task_id = 'add_posting_task',
        python_callable = add_posting,
        op_kwargs = {'table_name': 'ft_posting_f', 'path': PATH}
    )

    # Добавление записей md_account_d
    add_account_task = PythonOperator(
        task_id = 'add_account_task',
        python_callable = add_account,
        op_kwargs = {'table_name': 'md_account_d', 'path': PATH}
    )

    # Добавление записей md_currency_d
    add_currency_task = PythonOperator(
        task_id = 'add_currency_task',
        python_callable = add_currency,
        op_kwargs = {'table_name': 'md_currency_d', 'encoding': 'cp1252', 'path': PATH}
    )

    # Добавление записей md_exchange_rate_d
    add_exchange_rate_task = PythonOperator(
        task_id = 'add_exchange_rate_task',
        python_callable = add_exchange_rate,
        op_kwargs = {'table_name': 'md_exchange_rate_d', 'path': PATH}
    )

    # Добавление записей md_ledger_account_s
    add_ledger_account_task = PythonOperator(
        task_id = 'add_ledger_account_task',
        python_callable = add_ledger_account,
        op_kwargs = {'table_name': 'md_ledger_account_s', 'path': PATH}
    )

    end = PythonOperator(
        task_id = 'end',
        python_callable = end_log_entry,
        op_kwargs = {'table_name': 'log_table'},
        provide_context = True
        
    )

    # Порядок выполения задач
    (
        start
        >> sleep_task
        >> [add_balance_task, add_posting_task, add_account_task, add_currency_task, add_exchange_rate_task, add_ledger_account_task]
        >> end
    )