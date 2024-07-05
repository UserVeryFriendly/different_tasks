from PIL import Image
from psycopg2 import extras
from bot.bot import Bot
import configparser
import datetime
import pytz
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import os
import sys

current_script_directory = os.path.dirname(os.path.abspath(__file__))
image_dir = os.path.join(current_script_directory, 'images')


def calculate_table_status(result):
    if not result:
        return None

    all_results = []

    for row in result:
        dataflow_id = row.get('dataflow_id')
        table_name = row.get('table_name')
        periodicl = row.get('periodicl')
        update_timestamp = row.get('update_timestamp')
        status = row.get('status')

        current_result = (dataflow_id, table_name, periodicl, update_timestamp, status)
        all_results.append(current_result)

    return all_results

def draw_table_status(dataflow_id, table_name, periodicl, update_timestamp, status):
    if pd.isna(update_timestamp):
        formatted_timestamp_str = 'Данные не обновлялись'
    else:
        formatted_timestamp = pd.to_datetime(update_timestamp) - pd.Timedelta(hours=4)
        formatted_timestamp_str = formatted_timestamp.strftime('%Y-%m-%d %H:%M:%S')

    df = pd.DataFrame({
        'Поток': [dataflow_id],
        'Таблица': [table_name],
        'Период': [periodicl],
        'Последнее обновление': [formatted_timestamp_str],
        'Статус': [status]
    })

    return df


def read_config(file_path):
    config = configparser.ConfigParser()
    config.read(file_path)
    db_params = {
        'host': str(config['RTK_DB']['host']),
        'port': str(config['RTK_DB']['port']),
        'database': str(config['RTK_DB']['database']),
        'user': str(config['RTK_DB']['user']),
        'password': str(config['RTK_DB']['password']),
    }
    TOKEN = str(config['WARNING_BOT']['warning_bot'])
    api_url_base = str(config['WARNING_BOT']['api_url_base'])


    return db_params, TOKEN, api_url_base


def get_sql_query(db_params, inc, db_table):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor(cursor_factory=extras.DictCursor)

    sql_query = f"""
        SELECT * 
        FROM {db_table}
        WHERE incident_id = '{inc}'
        ORDER BY status DESC, dataflow_id ASC
    """

    cursor.execute(sql_query)
    result = cursor.fetchall()
    cursor.close()
    connection.close()
    return result

def draw_status_table(result, inc):
    '''Отрисовка таблицы'''
    colors = {'OK': '#008000', 'WARNING': '#8B0000', 'X': '#696969'}
    font_color = '#FFFFFF'
    frames = []

    for row in calculate_table_status(result):
        dataflow_id, table_name, periodicl, update_timestamp, status = row
        frames.append(draw_table_status(dataflow_id, table_name, periodicl, update_timestamp, status))

    final_df = pd.concat(frames, ignore_index=True)

    def get_cell_color(status):
        return colors.get(status, 'grey')

    fill_colors = [
        [get_cell_color(status) for status in final_df['Статус']]
    ]

    table_trace = go.Table(
        columnorder=[1, 2, 3, 4, 5],
        columnwidth=[70, 360, 70, 180, 70],
        header=dict(values=final_df.columns, line_color='black', fill_color='black', font=dict(color=font_color, size=21), align='left', height=28),
        cells=dict(
            values=[final_df[col] for col in final_df.columns],
            line_color='black',
            fill=dict(color='#696969'),
            font=dict(color=font_color, size=18),
            align='left',
            height=28,
            fill_color=fill_colors
        )
    )

    fig = go.Figure(data=[table_trace])

    fig.update_layout(
        autosize=True,
        margin=dict(l=0, r=0, b=0, t=0),
        paper_bgcolor='rgba(0,0,0,0)',
    )
    table_height = (len(final_df) * 28) + 28
    table_image_path = os.path.join(image_dir, f'{inc}_table.png')
    fig.write_image(table_image_path, width=1200, height=table_height)

    return table_image_path


def draw_status_pie(values, names, inc, db_table):
    '''Отрисовка бублика'''
    colors = {'OK': 'lime', 'WARNING': 'red', 'X': 'grey'}
    non_zero_values = [val for val in values if val != 0]
    non_zero_names = [name for val, name in zip(values, names) if val != 0]
    msk_tz = pytz.timezone('Europe/Moscow')
    current_time_msk = datetime.datetime.now(msk_tz).strftime("%Y-%m-%d %H:%M:%S")

    fig = px.pie(values=non_zero_values, names=non_zero_names, color=non_zero_names, color_discrete_map=colors)
    fig.update_layout({
        'plot_bgcolor': 'black',
        'paper_bgcolor': 'black',
        'font_color': 'white',
        'legend': {
            'x': 0.8,
            'y': 0.5,
            'title_text': 'Статусы:',
            'font': {'size': 38}
        }
    })
    fig.update_traces(textinfo='value', textfont_size=60, hole=0.7)

    inc_annotation = dict(
        showarrow=False,
        text=inc,
        x=0.5,
        y=0.5,
        font=dict(size=70, color='white'),
    )

    date_annotation = dict(
        showarrow=False,
        text=current_time_msk,
        x=0.5,
        y=0.40,
        font=dict(size=20, color='rgb(120, 120, 120)'),
    )

    table_annotation = dict(
        showarrow=False,
        text=db_table,
        x=0.5,
        y=0.60,
        font=dict(size=20, color='rgb(120, 120, 120)'),
    )

    fig.update_layout(annotations=[inc_annotation, date_annotation, table_annotation])

    buble_image_path = os.path.join(image_dir, f'{inc}_buble.png')
    fig.write_image(buble_image_path, width=1200, height=720)

    return buble_image_path

def calculate_status_counts(result):
    '''Расчет статусов'''
    status_counts = {'WARNING': 0, 'OK': 0, 'X': 0}

    for row in result:
        status = row.get('status')
        if status in status_counts:
            status_counts[status] += 1

    values = [count for count in status_counts.values()]
    names = list(status_counts.keys())

    return values, names

def send_message(token, api_url_base, chat_id, result, image_path):
    '''Отправка сообщения'''
    if not result:
        return

    bot = Bot(token=token, api_url_base=api_url_base, is_myteam=True)

    with open(image_path, 'rb') as file:
        bot.send_file(chat_id=chat_id, file=file, caption="")


def combine_images(bubble_image_path, table_image_path, inc):
    '''Объединение таблички и бублика'''
    bubble_image = Image.open(bubble_image_path)
    table_image = Image.open(table_image_path)
    width_bubble, height_bubble = bubble_image.size
    width_table, height_table = table_image.size
    new_width = max(width_bubble, width_table)
    new_height = height_bubble + height_table
    new_image = Image.new("RGB", (new_width, new_height), "black")
    new_image.paste(bubble_image, (0, 0))
    new_image.paste(table_image, (0, height_bubble))
    combined_image_path = os.path.join(image_dir, f'{inc}_combine.png')

    new_image.save(combined_image_path)
    return combined_image_path

def start_draw(inc, db_table, chat_id):
    db_params, TOKEN, api_url_base = read_config('/abc/config/global_config.cfg')
    result = get_sql_query(db_params, inc, db_table)

    if result:
        # Табличка
        table_image_path = draw_status_table(result, inc)
 
        # Бублик
        values, names = calculate_status_counts(result)
        buble_image_path = draw_status_pie(values, names, inc, db_table)
        

        combined_image_path = combine_images(buble_image_path, table_image_path, inc)

        os.remove(buble_image_path)
        os.remove(table_image_path)

        warning_count = values[names.index('WARNING')]

        if chat_id == None:
            return(combined_image_path)
        else:
            if warning_count == 0:
                pass
            else:
                send_message(TOKEN, api_url_base, chat_id, result, combined_image_path)
                return(combined_image_path)


if __name__ == "__main__":
    if len(sys.argv) == 4:  # 4 потому что первым аргументом считается сам скрипт
        inc = sys.argv[1]
        db_table = sys.argv[2]
        chat_id = sys.argv[3]

        start_draw(inc, db_table, chat_id)
    else:
        print('3 аргумента: incident_id, название вью, id чата(или None)')


'''
Для запуска из кода (без отправки в чат) можно пользовать такую конструкцию

from draw_bubble import *

combined_image_path = start_draw('TKT', 'tech.tech_control_view_status', None)
print(combined_image_path)

Код возвращает путь до файла на серве, далее можете что угодно с ним делать
'''
