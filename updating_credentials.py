import configparser
import psycopg2
from psycopg2 import extras


config = configparser.ConfigParser()
config.read('/abc/config/global_config.cfg')


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


def take_logins(connection):
    '''Забор логинов пользователей'''
    cursor = connection.cursor(cursor_factory=extras.DictCursor)
    sql_query = f'''
    select db_khd_nguk FROM nsi.employee_logins x
    order by db_khd_nguk asc
'''
    cursor.execute(sql_query)
    result = cursor.fetchall()
    connection.commit()
    cursor.close()

    logins = [item[0] for item in result]
    
    return logins


def update_cred(login, connection):
    '''Обновление полномочий в бд'''
    cursor = connection.cursor(cursor_factory=extras.DictCursor)

    # Запрос 1 - Разрешения USAGE на все несистемные схемы для определенного пользователя
    sql_query_1 = f'''
        DO
        $$
        DECLARE
            r RECORD;
        BEGIN
            FOR r IN SELECT schema_name FROM information_schema.schemata 
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema') 
            AND schema_name NOT LIKE 'pg_%'
            LOOP
                EXECUTE 'GRANT USAGE ON SCHEMA ' || quote_ident(r.schema_name) || ' TO {login};';
            END LOOP;
        END
        $$;
    '''

    # Запрос 2 - Разрешения SELECT на все несистемные таблицы для определенного пользователя
    sql_query_2 = f'''
        DO
        $$
        DECLARE
            r RECORD;
        BEGIN
            FOR r IN SELECT table_schema, table_name FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema') 
            AND table_schema NOT LIKE 'pg_%'
            LOOP
                EXECUTE 'GRANT SELECT ON ' || quote_ident(r.table_schema) || '.' || quote_ident(r.table_name) || ' TO {login};';
            END LOOP;
        END
        $$;
    '''

    try:    
        cursor.execute(sql_query_1)    
        cursor.execute(sql_query_2)
        print(f'Обновление полномочий для пользователя {login} прошли успешно')
    except Exception as e:
        print(f'Ошибка при обновлении полномочий для пользователя {login}:\n{e}')
    finally:
        connection.commit()
        cursor.close()


if __name__=='__main__':
    try:
        connection = connect_db()
        logins = take_logins(connection)
        for login in logins:
            update_cred(login, connection)
    finally:
        print('Полномочия выданы.')
        connection.close()
        