import requests
import json
import configparser
import psycopg2
import urllib3
import telebot

from bot.bot import Bot
from datetime import datetime, timedelta
from psycopg2 import extras


# Убираю лишние варнинги
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

config = configparser.ConfigParser()
config.read('/abc/config/global_config.cfg')

JIRA_URL = config['JIRA']['server']
USERNAME = config['JIRA']['user']  # Логин жиры
PASSWORD = config['JIRA']['password']  # Пароль жиры
PROJECT_KEY = 'IT04'  # pkey пространства
ISSUE_TYPE = 'Задача'  # Тип задачи
MAX_RETRIES = 3  # Максимальное количество попыток создания задачи


token_tg = config['ALARM_TG']['TELEGRAM_BOT_TOKEN']
chat_tg = '*******'
bot_tg = telebot.TeleBot(token_tg)

token_vk = config['TOKEN']['artifact_bot']
chat_vk = '***********'
api_url_base = 'https://api.new-mmc.ru/bot/v1'
bot_vk = Bot(token=token_vk, api_url_base=api_url_base, is_myteam=True)


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
        from tech.logs_issue_jira
        where stage = 'in progress'
        AND logger = 'logger_column'
'''  # Берем все id задач с этапом "in progress"
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
        verify=False
    )

    if response.status_code == 200:
        data = response.json()
        status_name = data.get('fields', {}).get('status', {}).get('statusCategory', {}).get('name')
        return status_name
    else:
        print('Error occurred:', response.text)


def change_status(id_task, connection):
    '''Замена статуса и этапа в лог таблице'''
    cursor = connection.cursor(cursor_factory=extras.DictCursor)
    sql_query = f'''
        UPDATE tech.logs_issue_jira
        SET stage = 'done',
            update_timestamp = CURRENT_TIMESTAMP
        WHERE id_task = '{id_task}'
'''  # Меняем статус задачи если, та выполнена
    cursor.execute(sql_query)
    connection.commit()
    cursor.close()


def get_sql_query(connection):
    '''Получение информации о созданных задачах с бд'''
    cursor = connection.cursor(cursor_factory=extras.DictCursor)

    sql_query = f"""
        SELECT n.nspname AS schema_name, 
            c.relname AS table_name,  
            el.jira AS owner_name,
            string_agg(a.attname, ', ') AS null_columns,
            el.name AS full_name
        FROM pg_class c 
        LEFT JOIN pg_namespace n ON n.oid = c.relnamespace 
        LEFT JOIN pg_attribute a ON a.attrelid = c.oid 
        LEFT JOIN pg_description d ON c.oid = d.objoid AND a.attnum = d.objsubid 
        LEFT JOIN pg_roles u ON c.relowner = u.oid 
        LEFT JOIN nsi.employee_logins el ON u.rolname = el.db_khd_nguk 
        WHERE c.relkind = 'r' 
            AND a.attnum > 0 
            AND d.description IS NULL 
            AND a.attname NOT IN ('update_timestamp', 'row_count') 
            AND a.attname NOT LIKE '%pg.dropped.%'
            AND u.rolname <> 'lanit'
            AND n.nspname <> 'pg_catalog'
            AND n.nspname <> 'information_schema'
            AND n.nspname <> 'dal_tikton'
			AND NOT EXISTS (
                SELECT 1 
                FROM tech.logs_issue_jira lij
                WHERE "name" = c.relname
                AND lij.stage = 'in progress'
                AND lij.logger = 'logger_column'
            )
        GROUP BY n.nspname, c.relname, el.jira, el.name
"""

    cursor.execute(sql_query)
    result = cursor.fetchall()
    cursor.close()
    return result


def create_task(schema_name, table_name, owner_name, null_columns, first_name):
    '''Создание задачи в Jira'''

    for attempt in range(MAX_RETRIES):
        try:
            current_datetime = datetime.now()
            due_date = current_datetime + timedelta(weeks=1)

            headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
            }

            data = {
                'fields': {
                    'project': {
                        'key': PROJECT_KEY
                    },
                    'summary': 'Проставить комментарии к колонкам',
                    'description': f'Необходимо проставить комментарии колонкам таблицы: {schema_name}.{table_name}. Колонки с пустым комментарием: {null_columns}',
                    'issuetype': {
                        'name': ISSUE_TYPE
                    },
                    'assignee': {
                        'name': owner_name
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
                print('Задача успешно создана и назначена на пользователя: ', owner_name)
                response_json = response.json()
                issue_id = response_json['id']
                print('ID созданной задачи:', issue_id)
                break  # Прерываем цикл, если задача успешно создана
            else:
                print(f'Произошла ошибка при создании задачи (попытка {attempt + 1}/{MAX_RETRIES}):', response.text)

        except Exception as e:
            print(f'Произошла ошибка при создании задачи (попытка {attempt + 1}/{MAX_RETRIES}):', str(e))

    return issue_id

def update_log_table(connection, table_name, issue_id):
    '''Обновление лог таблицы'''
    cursor = connection.cursor(cursor_factory=extras.DictCursor)
    sql_query = '''
    INSERT INTO tech.logs_issue_jira 
        ("name", "type", logger, stage, id_task, update_timestamp)
    VALUES (%s, 'table', 'logger_column', 'in progress', %s, CURRENT_TIMESTAMP);
'''  # Добавление в лог запись о задаче
    cursor.execute(sql_query, (table_name, issue_id))
    connection.commit()
    cursor.close()


if __name__ == '__main__':
    connection = connect_db()
    id_tasks = take_id_task(connection)
    for id_task in id_tasks:
        id_task = str(id_task['id_task'])
        status_name = check_task(id_task)
        if status_name == 'Выполнено':
            change_status(id_task, connection)
            print(f'Статус изменен для задачи: {id_task}')
        else:
            print(f'Статус "Не выполнена" для id_task: {id_task}')


    result = get_sql_query(connection)
    if not result:
        message = 'Все комментарии проставлены, задачи не созданы'
        print('Все комменты проставлены')
        bot_vk.send_text(chat_id=chat_vk, text=message)
        bot_tg.send_message(chat_tg, message)
    else:
        i = 0
        tasks_summary = {}
        for row in result:
            schema_name, table_name, owner_name, null_columns, first_name = row
            issue_id = create_task(schema_name, table_name, owner_name, null_columns, first_name)
            update_log_table(connection, table_name, issue_id)
            print('Лог таблица обновлена')
            i += 1
            if first_name in tasks_summary:
                tasks_summary[first_name] += 1
            else:
                tasks_summary[first_name] = 1
        print(f'Создано задач по null колонкам: {i}')
        message = "Созданы задачи для:\n"
        for user, count in tasks_summary.items():
            message += f"\n{user} - {count}"

        print(message)
        bot_vk.send_text(chat_id=chat_vk, text=message)
        bot_tg.send_message(chat_tg, message)

    connection.close()
