import requests
import signal
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

def handle_timeout(signum, frame):
    raise TimeoutError("Timeout reached!")


signal.signal(signal.SIGALRM, handle_timeout)
signal.alarm(120)  # Таймаут в секундах

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
    cursor = connection.cursor(cursor_factory=extras.DictCursor)
    sql_query = f'''
        select id_task
        from tech.logs_control_task
        where stage = 'in progress'
''' # Берем все id задач с этапом "in progress"
    cursor.execute(sql_query)
    result = cursor.fetchall()
    connection.commit()
    cursor.close()
    return result


def check_task(id_task):
    '''Проверка задач на выполнение'''
    headers = {
  "Accept": "application/json",
  "Content-Type": "application/json"
}
     
    response = requests.request(
    'GET',
     f'{JIRA_URL}/rest/api/2/issue/{id_task}',
     headers=headers,
     auth=(USERNAME, PASSWORD),
     verify=False)
    
    if response.status_code == 200:
        data = response.json()
        status_name = data.get('fields', {}).get('status', {}).get('statusCategory', {}).get('name')
        assignee_name = data.get('fields', {}).get('assignee', {}).get('displayName', {})

        comments = data.get('fields', {}).get('comment', {}).get('comments', [])
        if comments:
            comment_body = comments[0].get('body', '')
        else:
            comment_body = None

        return status_name, assignee_name, comment_body

    elif response.status_code == 404:
        status_name = 'Удалено'
        assignee_name = None
        comment_body = None
        return status_name, assignee_name, comment_body
    else:
        print('Error occurred:', response.text)

def change_del_status(id_task, stage, connection):
    """Замена статуса и этапа в лог таблице, если задача удалена"""
    try:
        sql_query = f'''
            UPDATE tech.logs_control_task
            SET stage = '{stage}',
                update_timestamp = CURRENT_TIMESTAMP
            WHERE id_task = '{id_task}'
        '''  # Меняем статус задачи если, та выполнена

        with connection.cursor(cursor_factory=extras.DictCursor) as cursor:
            cursor.execute(sql_query)

    except Exception as e:
        print(f'Error_change_status: {e}')


def change_status(id_task, assignee_name, comment_body, connection):
    '''Замена статуса и этапа в лог таблице'''
    cursor = connection.cursor(cursor_factory=extras.DictCursor)
    if comment_body == None:
        comment_body = 'NULL'
    else:
        comment_body = f"'{comment_body}'"

    sql_query = f'''
        UPDATE tech.logs_control_task
        SET status = 'OK',
            stage = 'done',
            assignee_name = '{assignee_name}',
            update_timestamp = CURRENT_TIMESTAMP,
            type_of_problem = {comment_body}
        WHERE id_task = '{id_task}'
''' # Меняем статус задачи если, та выполнена
    cursor.execute(sql_query)
    connection.commit()
    cursor.close()


def create_task(table_name, schema_name, incident_id, dataflow_id, periodicl, last_timestamp, data_engineer, status, nifi_url):
    '''Создание задачи в Jira'''

    message = (f'Необходимо проверить поток: {dataflow_id}. Группа потоков: {incident_id}\n' 
               f'Таблица: {schema_name}.{table_name}\n'
               f'Последнее обновление: {last_timestamp}. Периодичность: {periodicl}\n'
               f'Хост потока: {data_engineer}')
    if nifi_url:
        message += f'\nNiFi URL: {nifi_url}'

    for attempt in range(MAX_RETRIES):
        try:
            current_datetime = datetime.now()
            due_date = current_datetime + timedelta(days=3)

            headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
            }

            data = {
                'fields': {
                    'project': {
                        'key': PROJECT_KEY
                    },
                    'summary': '[ИКД] Поток находится в WARNING',
                    'description': message,
                    'issuetype': {
                        'name': ISSUE_TYPE
                    },
                    'assignee': {
                        'name': "molotkov.i.iu"
                    },
                    "reporter": {
                        "name": "mitrohin.p.i"
                    },
                    'duedate': str(due_date)
                }
            }

            response = requests.post(
                f'{JIRA_URL}/rest/api/2/issue/',
                headers=headers,
                data=json.dumps(data),
                auth=(USERNAME, PASSWORD),
                verify=False
            )

            if response.status_code == 201:
                response_json = response.json()
                issue_id = response_json['id']
                print('ID созданной задачи:', issue_id)
                break
            else:
                print(f'Произошла ошибка при создании задачи (попытка {attempt + 1}/{MAX_RETRIES}):', response.text)

        except Exception as e:
            print(f'Произошла ошибка при создании задачи (попытка {attempt + 1}/{MAX_RETRIES}):', str(e))
    return issue_id


def add_watcher(issue_id):
    '''Добавление наблюдателя'''
    headers = {
    "Accept": "application/json",
    "Content-Type": "application/json"
    }

    payload = json.dumps("molotkov.i.iu")

    response = requests.request(
       'POST',
       f'{JIRA_URL}/rest/api/2/issue/{issue_id}/watchers',
       data=payload,
       headers=headers,
       auth=(USERNAME, PASSWORD),
       verify=False
    )

    if response.status_code == 200 or response.status_code == 204:
        print("Наблюдатель добавлен")
    else:
        print(f"Ошибка при добавлении наблюдателя: {response.status_code}")


    return False


def update_log_table(connection, table_name, schema_name, incident_id, dataflow_id, periodicl, last_timestamp, data_engineer, status, issue_id):
    '''Обновление лог таблицы'''
    cursor = connection.cursor(cursor_factory=extras.DictCursor)
    sql_query = '''
    INSERT INTO tech.logs_control_task 
        (schema_name, table_name, incident_id, dataflow_id, periodicl, last_timestamp, status, stage, data_engineer, id_task, update_timestamp)
    VALUES (%s, %s, %s, %s, %s, %s, %s, 'in progress', %s, %s, CURRENT_TIMESTAMP);
''' # Добавление в лог запись о задаче
    cursor.execute(sql_query, (schema_name, table_name, incident_id, dataflow_id, periodicl, last_timestamp, status, data_engineer, issue_id))
    connection.commit()
    cursor.close()

def get_warning_status(connection):
    '''Получение информации о WARNING в бд'''
    cursor = connection.cursor(cursor_factory=extras.DictCursor)

    sql_query = f"""
    SELECT
        tcvs.table_name,
        tcvs.schema_name,
        tcvs.incident_id,
        tcvs.dataflow_id,
        tcvs.periodicl,
        tcvs.time_up::timestamp AS last_timestamp,
        tcvs.data_engineer,
        tcvs.status,
        tcvs.nifi_url,
        case when  tcvs.time_up::timestamp + interval '2 hour'< current_timestamp + interval '4 hour' then 1 else 0 end as flag_2_hour,
        current_timestamp
    FROM
        tech.tech_control_view_status tcvs
    WHERE
        tcvs.status = 'WARNING'
        AND tcvs.periodicl NOT LIKE '%МИН'
        AND NOT EXISTS (
            SELECT 1
            FROM tech.logs_control_task lct
            WHERE lct.table_name = tcvs.table_name
            AND lct.stage = 'in progress'
        )
""" # Не берем те задачи которые есть в таблице с этапом "in progress"

    cursor.execute(sql_query)
    result = cursor.fetchall()
    cursor.close()
    print(result)
    return result


if __name__=='__main__':
    try:
        connection = connect_db()
        id_tasks = take_id_task(connection)
        for id_task in id_tasks:
            id_task = str(id_task['id_task'])
            status_name, assignee_name, comment_body = check_task(id_task)
            if status_name == 'Выполнено':
                change_status(id_task, assignee_name, comment_body, connection)
                print(f'Статус задачи изменен на "done" для задачи: {id_task}')
            elif status_name == 'Удалено':
                change_del_status(id_task, 'deleted', connection)
                print(f'Статус задачи изменен на "deleted" для задачи: {id_task}')
            else:
                print(f'Статус "Не выполнена" для id_task: {id_task}')

        result = get_warning_status(connection)

        if not result:
            print('Всё ок')
        else:
            for row in result:
                table_name, schema_name, incident_id, dataflow_id, periodicl, last_timestamp, data_engineer, status, nifi_url, flag_2_hour, current_timestamp= row
                issue_id = create_task(table_name, schema_name, incident_id, dataflow_id, periodicl, last_timestamp, data_engineer, status, nifi_url)
                add_watcher(issue_id)
                print('Задача создана')
                update_log_table(connection, table_name, schema_name, incident_id, dataflow_id, periodicl, last_timestamp, data_engineer, status, issue_id)
                print('Лог таблица обновлена')
        
        signal.alarm(0)
    except TimeoutError as e:
        raise f'Ошибка таймаута: {str(e)}'
    except Exception as e:
        print(f'Произошла неизвестная ошибка: {str(e)}')
    finally:
        connection.close()
