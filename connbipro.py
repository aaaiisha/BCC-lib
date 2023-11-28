#!/usr/bin/env python
# coding: utf-8

# In[30]:


#Подключение к базе
import pandas as pd
import numpy as np
import os
import http.client
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from datetime import datetime
import requests
import psycopg2
import socket
import getpass
import time
import schedule


#Подключаемся в EDWSTB
# db_url_old = {
#     'port': 1521,
#     'host': '10.15.28.28',
#     'drivername': 'oracle',
#     'database': 'edwprod',
#     'username': 'suleymed',
#     'password': 'pkn$23458xzx'
# }

#Подключаемся в EDWDBSTB
db_url = {
    'port': 1522,
    'host': '10.15.28.74',
    'drivername': 'oracle',
    'database': 'edwdbstb',
    'username': 'suleymed',
    'password': 'Ch#ngeM3'
}


# engine_old = create_engine(URL(**db_url_old), encoding='utf8')

# engine_roma = create_engine(URL(**db_url_roma), encoding='utf8')

engine = create_engine(URL(**db_url), encoding='utf8')

os.environ["NLS_LANG"] = "Russian.AL32UTF8"

cnx = create_engine('postgresql://askarays:tyuRGFH#123@10.15.128.146:6432/dashdb')

cncrm = create_engine('postgresql://read_usr:haslS2#ha9@10.15.128.77:5444/ocrmdb')

sd = create_engine('postgresql://nausd4_ro:pa0Nixan#dFbn6@10.15.128.93:5432/nausd4')

# рассылка в чат с Продукт-оунерами
get = 'https://api.telegram.org/bot5688833061:AAEhah2fux6QLGGPGmjj2rJ99FRHyqp6VIg/sendMessage?chat_id=-1001700593932&text={ADD_TXT}'

# рассылка в чат для проверки работы ЛК Все
getlk = 'https://api.telegram.org/bot5660690907:AAFYpzTQrGbI3clfI7wF3FHQMTuzajs5_3c/sendMessage?chat_id=-835467748&text={ADD_TXT}'

# рассылка в чат для проверки работы ЛК РБ
getrb = 'https://api.telegram.org/bot6016738780:AAG2Fa8KnU8TWwrt6jaToOSxNSlhzDdseKs/sendMessage?chat_id=-1001841883814&text={ADD_TXT}'

# рассылка в чат с по логинам
getlogins = 'https://api.telegram.org/bot5720071685:AAE0a4IVJnrSxD45YMKubMHGiWxI0BOdpBI/sendMessage?chat_id=-1001897348534&text={ADD_TXT}'

# рассылка в чат с ДРшками
getbdays = 'https://api.telegram.org/bot5923682214:AAG7glzHKfVeMnjZMpn3vj99P4xrHJ1Fvuc/sendMessage?chat_id=-974060360&text={ADD_TXT}'

# рассылка в чат для Бальной
chatballsys = 'https://api.telegram.org/bot6364051763:AAFkPVtzzh9jlDASOQ5HS5eT5sd7YViLuN8/sendMessage?chat_id=-933288841&text={ADD_TXT}'  

def timezone_pbi(date_str):
    import datetime
    import pytz
    local_tz = pytz.timezone('Asia/Almaty')  # Примерно соответствует GMT+6
    naive_dt = datetime.datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    utc_dt = pytz.utc.localize(naive_dt)
    local_dt = utc_dt.astimezone(local_tz)
    return local_dt.strftime("%d-%m-%Y, %H:%M:%S")


def chat_ball_sys_req_prod(message):
    requests.get(get.format(ADD_TXT = message), proxies={
    "http": "http://10.1.246.2:8080", "https": "http://10.1.246.2:8080", })

def chat_ball_sys_req(message):
    requests.get(chatballsys.format(ADD_TXT = message), proxies={
    "http": "http://10.1.246.2:8080", "https": "http://10.1.246.2:8080", })
    
def insert_data_with_timestamp2(name_of_table, name_of_product, name_def, script, connector_use,editor,comment):
    editor=get_current_user()
    if not editor or not comment:
        print("Ошибка: Поля editor и comment должны быть заполнены.")
        return

    try:
        # Подключение к базе данных
        conn = psycopg2.connect(
            host='10.15.129.200',
            port=11530,
            dbname='dashdb',
            user='askarays',
            password='tyuRGFH#123'
        )

        # Создание курсора
        cursor = conn.cursor()

        # Загрузка данных в таблицу
        insert_data_query = '''
        INSERT INTO motiv."BI_SCRIPTS_CHRON" (rpt_base_ymd, name_of_table, name_of_product, name_def, script, connector_use, editor, comment)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        '''
        current_datetime = datetime.now()

        data = (current_datetime, name_of_table, name_of_product, name_def, script, connector_use,editor, comment)

        cursor.execute(insert_data_query, data)
        conn.commit()
        print("Данные успешно загружены в таблицу. Обновление:" + str(current_datetime))
        
        #айпишник,если фейково зальют с чужим логином
        local_ip = get_ip_address()
        
        # Вставка данных в новую таблицу логов
        insert_log_query = '''
        INSERT INTO motiv."LOG_TABLE" (time,log,ip)
        VALUES (%s, %s, %s)
        '''
        log_data = (current_datetime, f'Загрузка данных в таблицу BI_SCRIPTS_CHRON продукта {name_of_product}.Ответсвенный {editor}',local_ip)
        cursor.execute(insert_log_query, log_data)
        conn.commit()

        # Закрытие курсора и соединения
        cursor.close()
        conn.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Ошибка при загрузке данных:", error)
        
def get_ip_address():
    try:
        local_ip = socket.gethostbyname(socket.gethostname())
        return local_ip
    except Exception as e:
        print(f"Ошибка при получении локального IP-адреса: {e}")
        return None
    


def get_current_user():
    return getpass.getuser()

def insert_data_with_timestamp(name_of_table, name_of_product, name_def, script,connector_use):
    try:
        # Подключение к базе данных
        conn = psycopg2.connect(
            host='10.15.129.200',
            port=11530,
            dbname='dashdb',
            user='askarays',
            password='tyuRGFH#123'
        )

        # Создание курсора
        cursor = conn.cursor()

        # Загрузка данных в уже существующую таблицу
        insert_data_query = '''
        INSERT INTO motiv."BI_SCRIPTS_CHRON" (rpt_base_ymd,name_of_table,name_of_product,name_def,script,connector_use)
        VALUES (%s, %s, %s, %s, %s,%s)
        '''
        current_datetime = datetime.now()

        data = (current_datetime,name_of_table, name_of_product, name_def, script,connector_use)

        cursor.execute(insert_data_query, data)
        conn.commit()
        print("Данные успешно загружены в таблицу. Обновление:" + str(current_datetime))

        # Закрытие курсора и соединения
        cursor.close()
        conn.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Ошибка при загрузке данных:", error)

def getdata(name_of_table,name_of_product):
    try:
        # Подключение к базе данных
        conn = psycopg2.connect(
            host='10.15.128.146',
            port=6432,
            dbname='dashdb',
            user='askarays',
            password='tyuRGFH#123'
        )
        # Создание курсора
        cursor = conn.cursor()

        # Выборка данных из таблицы с фильтром по имени таблицы и продукта name_of_product
        select_data_query = '''
           select t.script from motiv."BI_SCRIPTS_CHRON" t where 1=1 
           and t."rpt_base_ymd"= (SELECT MAX(t."rpt_base_ymd") 
           FROM motiv."BI_SCRIPTS_CHRON" t where t."name_of_table"=%s and t."name_of_product"=%s) 
        '''

        cursor.execute(select_data_query, (name_of_table, name_of_product))

        # Получение всех значений столбца
        text_data_value = cursor.fetchone()

        # Закрытие курсора и соединения
        cursor.close()
        conn.close()

        return text_data_value[0] if text_data_value else None
    except (Exception, psycopg2.DatabaseError) as error:
        print("Ошибка при получении данных:", error)

def fetch_google_sheet_data(spreadsheet_id, gid):
    import pandas as pd
    import requests
    from io import StringIO
    import warnings
    
    warnings.filterwarnings('ignore', message='Unverified HTTPS request')
    
    base_url = "https://docs.google.com/spreadsheets/d"
    url = f"{base_url}/{spreadsheet_id}/export?format=csv&gid={gid}"
    response = requests.get(url, proxies={
                            "http": "http://10.1.246.2:8080"
                          , "https": "http://10.1.246.2:8080", }, verify =False
                           )
    if response.status_code != 200:
        raise ValueError(f"Failed to fetch data. Status code: {response.status_code}")
    csv_raw = StringIO(response.content.decode('utf-8'))
    return pd.read_csv(csv_raw)

def kpi_of_direction():
    import datetime as dt
    from jira import JIRA
    import pandas as pd
    import numpy as np
    now = dt.datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S")
    dt_for_chat = now.strftime("%d.%m.%Y")
    jira_options = {'server': 'https://jira.bcc.kz'}
    jira = JIRA(options=jira_options, token_auth='Mzg1OTM5MDUwNDkyOt7E9ds1wtoW+sPsPsL8eub64he0')

    data = {
        'Код проекта': [],
        'Код Основной задачи': [],
        'Код подзадачи': [],
        'Название задачи': [],
        'Тип': [],
        'Статус': [],
        'Приоритет': [],
        'Решение': [],
        'Метки': [],
        'ЗНИ': [],
        'Компоненты': [],
        'Описание': [],
        'Подзадачи': [],
        'Исполнитель': [],
        'Автор': [],
        'Информация об авторе': [],
        'Заказчик': [],
        'Категория задачи': [],
        'План': [],
        'Аналитик': [],
        'Разработчик': [],
        'Тестировщик': [],
        'Создано': [],
        'Обновлено': [],
        'Дата начала анализа:': [],
        'Дата окончания анализа:': [],
        'Дата начала разработки:': [],
        'Дата окончания разработки:': [],
        'Дата начала тестирования:': [],
        'Дата окончания тестирования:': [],
        'Дата начала приемки:': [],
        'Дата окончания приемки:': [],
        'Срок исполнения': [],
        'Оценка времени': [],
        'Дата резолюции': []
    }

    for issue in jira.search_issues('project=BD order by created desc', maxResults=0):
        data['Код проекта'].append(str(issue.fields.project) + '-' + str(issue.fields.project.name))
        try:
            data['Код Основной задачи'].append(str(issue.fields.parent))
        except:
            data['Код Основной задачи'].append(str(issue.key))
        data['Код подзадачи'].append(str(issue.key))
        data['Название задачи'].append(issue.fields.summary)
        data['Тип'].append(str(issue.fields.issuetype))
        data['Статус'].append(str(issue.fields.status))
        data['Приоритет'].append(str(issue.fields.priority))
        data['Решение'].append(str(issue.fields.resolution))
        labels = ','.join(map(str, issue.fields.labels))
        data['Метки'].append(labels)
        data['ЗНИ'].append(str(issue.fields.customfield_10204))
        components = ','.join(map(str, issue.fields.components))
        data['Компоненты'].append(components)
        data['Описание'].append(str(issue.fields.description))
        subtasks = ','.join(map(str, issue.fields.subtasks))
        data['Подзадачи'].append(subtasks)
        data['Исполнитель'].append(str(issue.fields.assignee))
        data['Автор'].append(str(issue.fields.reporter))
        try:
            data['Информация об авторе'].append(str(issue.fields.customfield_11058))
        except:
            data['Информация об авторе'].append(None)
        data['Заказчик'].append(str(issue.fields.customfield_10205))
        data['Категория задачи'].append(str(issue.fields.customfield_10202))
        data['План'].append(str(issue.fields.customfield_11014))
        try:
            data['Аналитик'].append(str(issue.fields.customfield_11400))
        except:
            data['Аналитик'].append(None)
        try:
            data['Разработчик'].append(str(issue.fields.customfield_11401))
        except:
            data['Разработчик'].append(None)
        try:
            data['Тестировщик'].append(str(issue.fields.customfield_11402))
        except:
            data['Тестировщик'].append(None)
        data['Создано'].append(str(issue.fields.created))
        data['Обновлено'].append(str(issue.fields.updated))
        data['Дата начала анализа:'].append(str(issue.fields.customfield_11008))
        data['Дата окончания анализа:'].append(str(issue.fields.customfield_11017))
        data['Дата начала разработки:'].append(str(issue.fields.customfield_11015))
        data['Дата окончания разработки:'].append(str(issue.fields.customfield_11016))
        data['Дата начала тестирования:'].append(str(issue.fields.customfield_11019))
        data['Дата окончания тестирования:'].append(str(issue.fields.customfield_11018))
        data['Дата начала приемки:'].append(str(issue.fields.customfield_11011))
        data['Дата окончания приемки:'].append(str(issue.fields.customfield_11012))
        try:
            data['Срок исполнения'].append(str(issue.fields.duedate))
        except:
            data['Срок исполнения'].append(None)
        try:
            data['Оценка времени'].append(str(issue.fields.customfield_10106))
        except:
            data['Оценка времени'].append(None)
        try:
            data['Дата резолюции'].append(str(issue.fields.resolutiondate))
        except:
            data['Дата резолюции'].append(None)

    df = pd.DataFrame(data)
    df['rpt_base_ymd'] = dt_string
    del_jira = """delete from motiv."JIRA_PARSE_BI" """
    with cnx.begin() as conn:
        conn.execute(del_jira)
    # Запись данных в PostgreSQL
    df.to_sql("JIRA_PARSE_BI", con=cnx, schema='motiv', if_exists="append", index=False)
    message = 'Данные по JIRA задачам обновлены. Дата обновления: '+str(dt_for_chat)
    chat_ball_sys_req_prod(message)
    
class Dashboard:

    def __init__(self, dataset, group,name):
        self.dataset = dataset    
        self.group = group
        self.name = name
lk_motiv = Dashboard("11b14eb0-9eea-442b-9990-d3a3ca950190", 
                     "3b7980e7-be95-46bf-bf53-0dd5d69e8f70",
                     "Личный кабинет в Раб.обл. Мотивации МСБ");
lk_bcc   = Dashboard("72ac7b4f-7273-406f-8a7f-d927afa51eb6",
                     "435eadfd-345b-40b8-a2d6-c83f52346d99",
                    "Личный кабинет в Раб.обл. Личный кабинет");
lk_ball  = Dashboard("5fd5282a-6a86-4dfe-91cb-9a66a93f4c05", 
                     "435eadfd-345b-40b8-a2d6-c83f52346d99",
                    "Личный кабинет Баллы в Раб.обл. Личный кабинет");
kpi      = Dashboard("32137d79-09d3-4f25-9786-b3ee311cfca8", 
                     "3b7980e7-be95-46bf-bf53-0dd5d69e8f70",
                    "Направление KPI в Раб.обл. Мотивации МСБ");


def access_token_powerbi():
    import requests
    import pandas as pd
    import warnings

    warnings.filterwarnings('ignore', message='Unverified HTTPS request')

    tenant_id = '7f7b9357-9c44-4410-95df-2c59b7c1872b'
    client_id = '5b544ddd-07ce-4a84-b343-a585679d57ae'
    client_secret = 'BDB8Q~pbm_OH1F0bGIeE5l3LNeqg-3n0JquhHaD2'
    resource_url = "https://analysis.windows.net/powerbi/api"
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    data = {
        'grant_type': 'password',
        'client_id': client_id,
        'client_secret': client_secret,
        'resource': resource_url,
        'username': 'dash@bcc.kz',
        'password': 'Board2023!'
    }
    response = requests.post(token_url, data=data, verify=False)
    token_data = response.json()
    access_token = token_data['access_token']
    return access_token


def update_powerbi(groups,datasets,name):
    import requests
    import warnings
    warnings.filterwarnings('ignore', message='Unverified HTTPS request')

    tenant_id = '7f7b9357-9c44-4410-95df-2c59b7c1872b'
    client_id = '5b544ddd-07ce-4a84-b343-a585679d57ae'
    client_secret = 'BDB8Q~pbm_OH1F0bGIeE5l3LNeqg-3n0JquhHaD2'
    resource_url = "https://analysis.windows.net/powerbi/api"
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    data = {
        'grant_type': 'password',
        'client_id': client_id,
        'client_secret': client_secret,
        'resource': resource_url,
        'username': 'dash@bcc.kz',  # Email вашего аккаунта Power BI
        'password': 'Board2023!'   # Пароль вашего аккаунта Power BI
    }

    response = requests.post(token_url, data=data, proxies={
                            "http": "http://10.1.246.2:8080"
                          , "https": "http://10.1.246.2:8080", }, verify=False)
    token_data = response.json()
    access_token = token_data['access_token']
    refresh_url = 'https://api.powerbi.com/v1.0/myorg/groups/'+groups+'/datasets/'+datasets+'/refreshes'

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    response = requests.post(refresh_url, headers=headers, proxies={
                            "http": "http://10.1.246.2:8080"
                          , "https": "http://10.1.246.2:8080", }, verify=False)
    # Проверка ответа
    if response.status_code == 202:
        message = 'Обновление запущено для дашборда: '+name
        chat_ball_sys_req_prod(message)
        chat_ball_sys_req(message)
    else:
        message = 'Ошибка при обновлении дашборда: '+name
        chat_ball_sys_req_prod(message)
        chat_ball_sys_req(message)

def check_updt_powerbi(groups, datasets,name):
    import requests
    import warnings
    import datetime as dt 
    now = dt.datetime.now()
    dt_string = now.strftime("%d-%m-%Y, %H:%M:%S")
    
    warnings.filterwarnings('ignore', message='Unverified HTTPS request')

    tenant_id = '7f7b9357-9c44-4410-95df-2c59b7c1872b'
    client_id = '5b544ddd-07ce-4a84-b343-a585679d57ae'
    client_secret = 'BDB8Q~pbm_OH1F0bGIeE5l3LNeqg-3n0JquhHaD2'
    resource_url = "https://analysis.windows.net/powerbi/api"
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    data = {
        'grant_type': 'password',
        'client_id': client_id,
        'client_secret': client_secret,
        'resource': resource_url,
        'username': 'dash@bcc.kz',  # Email вашего аккаунта Power BI
        'password': 'Board2023!'   # Пароль вашего аккаунта Power BI
    }

    response = requests.post(token_url, data=data, proxies={
                            "http": "http://10.1.246.2:8080"
                          , "https": "http://10.1.246.2:8080", },verify=False)
    token_data = response.json()
    access_token = token_data['access_token']
    check_update_url = 'https://api.powerbi.com/v1.0/myorg/groups/'+groups+'/datasets/'+datasets+'/refreshes'

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    refresh_response = requests.get(check_update_url, headers=headers, proxies={
                            "http": "http://10.1.246.2:8080"
                          , "https": "http://10.1.246.2:8080", },verify=False)
    # Проверка ответа
    if refresh_response.status_code == 200:
        refresh_data = refresh_response.json()
        start_time = refresh_data['value'][0]['startTime']
        end_time = refresh_data['value'][0]['endTime']
        status = refresh_data['value'][0]['status']
        
        message = ('Время запуска обновления дашборда: '+name +', '
                   + timezone_pbi(start_time))
        chat_ball_sys_req_prod(message)
        chat_ball_sys_req(message)
        
    # Проверка на пустое значение endTime
        if not end_time:
            message = (name+" ещё обновляется. Время проверки: " + dt_string)
            chat_ball_sys_req_prod(message)
            chat_ball_sys_req(message)

        else:
            message = ('Время завершения обновления дашборда: '
                       +name +', '+timezone_pbi(end_time) 
                       + ' Статус дашборда:' + status)
            
            chat_ball_sys_req_prod(message)
            chat_ball_sys_req(message)

    else:
        message =("Ошибка при получении данных о последнем обновлении дашборда:" + name 
                  +" Время проверки: " + dt_string)
        
        chat_ball_sys_req_prod(message)
        chat_ball_sys_req(message)
        
    if refresh_response.status_code == 200:
        refresh_data = refresh_response.json()
        status = refresh_data['value'][0]['status']
        return status
    else:
        return 'Error'    
    
    
# функция заливки данных
def run_insert_postgre(dt_rep,dt_repd,name_of_function,name_of_table,connector):
            
    current_datetime_strt = dt.datetime.now()
    try:
        main_q =getdata(name_of_table,name_of_function).format(DATE_VALUE1=dt_rep)
    except Exception as e:
        message = f"Ошибка при получении данных из функции getdata: {e} " + name_of_function
        conn_help.chat_ball_sys_req(message)
        return
    if (connector =='ORACLE'):
        main_q = pd.read_sql(main_q.replace('#NEWLINE#', '\n'),conn_help.engine)
    else:
        main_q = pd.read_sql(main_q.replace('#NEWLINE#', '\n'),conn_help.cnx)
        
    message_cnt_rows = ' Кол-во строк: ' + str(len(main_q))
    current_datetime_end = dt.datetime.now()
    if len(main_q)==0:
        sql_updt =  """
                    update motiv.{NAME_OF_TABLE_Q}
                    set base_ymd = '{DATE_VALUE1}'
                    where 1=1 
                    and base_ymd <= '{DATE_VALUE1}'
                    and substr(base_ymd,1,6) = substr('{DATE_VALUE1}',1,6)
                    and name_of_func = '{NAME_OF_FUNC_Q}'
                    """.format(DATE_VALUE1=dt_rep,
                               NAME_OF_FUNC_Q =name_of_function,
                               NAME_OF_TABLE_Q = name_of_table
                              )
        with conn_help.cnx.begin() as conn:
            conn.execute(sql_updt)
    else:
        sql_del = """delete from motiv.{NAME_OF_TABLE_Q}
                       where base_ymd <= '{DATE_VALUE1}'
                       and substr(base_ymd,1,6) = substr('{DATE_VALUE1}',1,6)
                       and name_of_func = '{NAME_OF_FUNC_Q}'
                   """.format(DATE_VALUE1=dt_rep,
                              NAME_OF_FUNC_Q =name_of_function,
                              NAME_OF_TABLE_Q = name_of_table
                             )
        with conn_help.cnx.begin() as conn:
            conn.execute(sql_del)
    
    dt_string = current_datetime_end.strftime("%Y%m%d%H%M%S")
     
    main_q['rpt_base_ymd'] = dt_string
    main_q['name_of_func'] = name_of_function
    
    main_q.columns = main_q.columns.str.lower()
    main_q.to_sql(name_of_table,con=conn_help.cnx, schema='motiv' ,if_exists="append",index=False)
    
    seconds = (current_datetime_end - current_datetime_strt).seconds
    minutes = ((current_datetime_end - current_datetime_strt).seconds // 60)
    
    date_time = str(current_datetime_end.strftime("%Y-%m-%d %H:%M:%S"))
    message_text = '.\n 😎 Залита функция ' + name_of_function
    message_dt_rep = ', Дата отчёта: '+ str(dt_repd.strftime("%d.%m.%Y"))
    time_diff = (' Время работы: '+str(minutes) +' min. ' + str(seconds) +' sec.' )
    
    message = date_time+message_text+message_dt_rep+time_diff+message_cnt_rows
    conn_help.chat_ball_sys_req(message)
    
    
    
#функция для паралельного запуска функций 
def run_parallel_cells(df):
    # Разделяем датафрейм на части по 25 строк
    current_datetime_strt = dt.datetime.now()
    message = str(current_datetime_strt) + ' Начинаем загрузку данных для Бальной мотивации'
    conn_help.chat_ball_sys_req(message)
    
    df_splits = np.array_split(df, len(df) // 25 + 1)

    for split_index, df_chunk in enumerate(df_splits):
        with ThreadPoolExecutor(max_workers=25) as executor:
            current_datetime_strt = dt.datetime.now()
            futures_clients = []

            # Создаем futures на основе строк текущей части датафрейма
            for _, row in df_chunk.iterrows():
                future = executor.submit(run_insert_postgre,
                                         dt_rep,
                                         dt_repd,
                                         row['name_of_def_q'],
                                         row['name_of_table'],
                                         row['connector'])
                futures_clients.append(future)
                
            for future in futures_clients:
                try:
                    result = future.result()
                    if result is not None:
                        # Обработка результата, если это необходимо
                        pass
                except Exception as e:
                    func_name = future.fn.__name__
                    message_error =' Описание ошибки: ' f"Error in one of the futures: {str(e)} "
                    message ='Ошибка скрипта функции: '+ func_name + message_error
                    conn_help.chat_ball_sys_req(message)
            current_datetime_end = dt.datetime.now()
            date_time = current_datetime_end.strftime("%Y-%m-%d %H:%M:%S")

            seconds = (current_datetime_end - current_datetime_strt).seconds
            minutes = (seconds // 60)
            time_diff = ' Время работы: ' + str(minutes) + ' min. ' + str(seconds % 60) + ' sec.'
            message_nm = '.\n 😎 Поток ' + str((split_index+1)) + ',25-ти функций завершен. '
            message = str(date_time) + message_nm + time_diff
            conn_help.chat_ball_sys_req(message)
            
            
def log_result_table (name_of_table,name_of_function,dt_report,
                      date_report, connector, row_count, message,
                      time_minutes,time_seconds,used_script,rpt_base_ymd):
    with conn_help.cnx.begin() as conn:
        conn.execute("""
            INSERT INTO motiv.function_details_logs (
            name_of_table,name_of_function,dt_report,
            date_report, connector, row_count, message,
            time_minutes,time_seconds,used_script,rpt_base_ymd
            )
            VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)
        """, (name_of_table,name_of_function,dt_report,
              date_report, connector, row_count, message,
              time_minutes,time_seconds,used_script,rpt_base_ymd
             )
                    )


# In[24]:


# spreadsheet_id = "1NFfqQgE7nk4AvPUTXGWuwAAIAZc01VED"
# gid = "1397145094"
# df = fetch_google_sheet_data(spreadsheet_id, gid)
# df


# In[31]:


# update_powerbi(lk_motiv.group, lk_motiv.dataset,lk_motiv.name)
# update_powerbi(lk_bcc.group, lk_bcc.dataset,lk_bcc.name)
# update_powerbi(lk_ball.group, lk_ball.dataset,lk_ball.name)
# check_updt_powerbi(kpi.group, kpi.dataset,kpi.name)


# In[ ]:




