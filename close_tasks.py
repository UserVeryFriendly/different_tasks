import configparser

import psycopg2
import requests
from psycopg2 import extras


config = configparser.ConfigParser()
config.read('/abc/config/global_config.cfg')


JIRA_URL = config['JIRA']['server']
USERNAME = config['JIRA']['user']  # Логин жиры
PASSWORD = config['JIRA']['password']  # Пароль жиры
PROJECT_KEY = 'IT04'  # pkey пространства
ISSUE_TYPE = 'Задача'  # Тип задачи
MAX_RETRIES = 3  # Максимальное количество попыток создания задачи


def connect_db():
    '''Коннект к бд'''
    connection = None
    try:
        db_params = {
            'host': str(config['RTK_DB']['host']),
            'port': str(config['RTK_DB']['port']),
            'database': str(config['RTK_DB']['database']),
            'user': str(config['RTK_DB']['user']),
            'password': str(config['RTK_DB']['password']),
        }
        connection = psycopg2.connect(**db_params)
        return connection
    except Exception as e:
        print(f'Error_connect_db: {e}')
        if connection:
            connection.close()


def take_id_task(connection):
    """Забор id задач из лог таблицы"""
    try:
        sql_query = f'''
            select id_task
            from tech.logs_issue_jira
            where stage = 'in progress'
        '''  # Берем все id задач с этапом "in progress"

        with connection.cursor(cursor_factory=extras.DictCursor) as cursor:
            cursor.execute(sql_query)
            result = cursor.fetchall()

        return result
    except Exception as e:
        print(f'Error_take_id_task: {e}')


def check_task(task, connection):
    """Проверка задач на выполнение"""
    response = None
    try:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

        response = requests.request(
            'GET',
            f'{JIRA_URL}/rest/api/2/issue/{task}',
            headers=headers,
            auth=(USERNAME, PASSWORD),
            verify=False
        )

        if response.status_code == 404:
            change_status(task, 'deleted', connection)
            print(f'Меняем статус на "удален" для: {task}')
        elif response.status_code == 200:
            data = response.json()
            status = data.get('fields', {}).get('status', {}).get('statusCategory', {}).get('name')

            if status == 'Выполнено':
                change_status(task, 'done', connection)
                print(f'Статус изменен на "done" для задачи: {task}')
            else:
                print(f'Оставляем в работе id_task: {task}')
        else:
            print('Other response: ', response.status_code)
    except Exception as e:
        print(f'Error_check_tasks: {e}')
        if response:
            print(f'Response_code: {response.status_code}')
            print(f'Response: {response.content}')


def change_status(id_task, stage, connection):
    """Замена статуса и этапа в лог таблице"""
    try:
        sql_query = f'''
            UPDATE tech.logs_issue_jira
            SET stage = '{stage}',
                update_timestamp = CURRENT_TIMESTAMP
            WHERE id_task = '{id_task}'
        '''  # Меняем статус задачи если, та выполнена

        with connection.cursor(cursor_factory=extras.DictCursor) as cursor:
            cursor.execute(sql_query)

    except Exception as e:
        print(f'Error_change_status: {e}')

if __name__ == '__main__':
    conn = None
    try:
        conn = connect_db()
        with conn:
            id_tasks = take_id_task(conn)
            for id_task in id_tasks:
                id_task = str(id_task['id_task'])
                check_task(id_task, conn)
        if not id_tasks:
            print('Все задачи в статусе "done"')
    except Exception as e:
        print(f'Error_main: {e}')
    finally:
        if conn:
            conn.close()
