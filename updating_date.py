import requests
import json
import configparser
import psycopg2
from psycopg2 import extras
from datetime import datetime, timedelta


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
    db_params = {
    'host': str(config['RTK_DB']['host']),
    'port': str(config['RTK_DB']['port']),
    'database': str(config['RTK_DB']['database']),
    'user': str(config['RTK_DB']['user']),
    'password': str(config['RTK_DB']['password']),
}
    connection = psycopg2.connect(**db_params)

    return connection


def take_id_task(connection):
    '''Забор id задач из лог таблицы'''
    today = datetime.today().strftime('%Y-%m-%d')
    cursor = connection.cursor(cursor_factory=extras.DictCursor)
    sql_query = f'''
        select id_task
        from tech.logs_control_task
        where update_timestamp::text like '{today}%' and stage = 'done'
'''
    cursor.execute(sql_query)
    result = cursor.fetchall()
    connection.commit()
    cursor.close()

    return result


def change_status(id_task, update_timestamp, connection):
    '''Замена статуса и этапа в лог таблице'''
    cursor = connection.cursor(cursor_factory=extras.DictCursor)
    sql_query = f'''
        UPDATE tech.logs_control_task
        SET update_timestamp = '{update_timestamp}'
        WHERE id_task = '{id_task}'
'''
    cursor.execute(sql_query)
    connection.commit()
    cursor.close()


def take_timestamp(id_task, connection):
    cursor = connection.cursor(cursor_factory=extras.DictCursor)
    sql_query = f'''
    select resolutiondate
    from dal_jira.dal_jira_001_jiraissue
    where id = {id_task}
'''
    cursor.execute(sql_query)
    result = cursor.fetchall()
    connection.commit()
    cursor.close()

    return result[0][0]


if __name__=='__main__':
    connection = connect_db()
    id_tasks = take_id_task(connection)

    for attempt in range(MAX_RETRIES):
        try:
            for id_task in id_tasks:
                id_task = str(id_task['id_task'])
                update_timestamp = take_timestamp(id_task, connection)

                if update_timestamp:
                    change_status(id_task, update_timestamp, connection)
                    print('Статус изменен:', id_task)
                else:
                    print('Таймстемп не найден для:', id_task)
            break
        except Exception as e:
            print(f'Произошла ошибка при создании задачи (попытка {attempt + 1}/{MAX_RETRIES}):', str(e))
    
    connection.close()
