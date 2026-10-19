"""
DAG для генерации, проверки и загрузки данных в БД
"""
from airflow import DAG
from datetime import date, datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandera as pa
import pandas as pd 

import random
import uuid
import string

# Определяем набор символов (строчные буквы)
CHARACTERS = string.ascii_lowercase
ADJECTIVES = ["happy", "brave", "clever", "swift", "calm", "bright"]
NOUNS = ["tiger", "falcon", "river", "stone", "cloud", "wolf"]
DOMENS = ["example", "google", "mail"]

action_types = {
    'FIRST_VISIT': 1,
    'REGISTER': 2,
    'LOGIN': 3,
    'LOGOUT': 4,
    'CREATE_TOPIC': 5,
    'ENTRY_TOPIC': 6,
    'DELETE_TOPIC': 7, 
    'CREATE_MESSAGE':8
}

def random_datetime_in_month(year: int, month: int) -> datetime:
    """
    Функция возвращается случайный datetime за текущий месяц
    """
    # Первый день месяца
    start = datetime(year, month, 1)
    
    # Первый день следующего месяца
    if month == 12:
        end = datetime(year + 1, 1, 1)
    else:
        end = datetime(year, month + 1, 1)
    
    # Случайное количество секунд в диапазоне [0, длительность месяца)
    delta_seconds = int((end - start).total_seconds())
    if delta_seconds <= 0:
        random_seconds = 0
    else:
        random_seconds = random.randrange(delta_seconds)
    
    return start + timedelta(seconds=random_seconds)

def random_datetime_in_month_from(min_datetime: datetime) -> datetime:
    """
    Функция возвращается случайный datetime
    """
    
    # Первый день следующего месяца
    if min_datetime.month == 12:
        end = datetime(min_datetime.year + 1, 1, 1)
    else:
        end = datetime(min_datetime.year, min_datetime.month + 1, 1)
    
    # Случайное количество секунд в диапазоне [0, длительность месяца)
    delta_seconds = int((end - min_datetime).total_seconds())
    if delta_seconds <= 0:
        random_seconds = 0
    else:
        random_seconds = random.randrange(delta_seconds)
    
    return min_datetime + timedelta(seconds=random_seconds)

def random_datetime_in_day(min_datetime: datetime) -> datetime:
    """
    Функция возвращает случайный datetime этого же дня, больший чем заданный в min_datetime
    """
    # Ставлю + 1 sec чтобы исключить возможности совпадения min_datetime и новой полученной даты
    min_datetime += timedelta(seconds=1)
    next_day_start = (min_datetime + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    delta_seconds = int((next_day_start - min_datetime).total_seconds())
    if delta_seconds <= 0:
        random_seconds = 0
    else:
        random_seconds = random.randrange(delta_seconds)

    return min_datetime + timedelta(seconds=random_seconds)

def random_datetime_in_interval(min_datetime: datetime, max_datetime: datetime) -> datetime:
    """
    Функция возвращает случайный datetime из диапазона
    """

    # Ставлю + 1 sec чтобы исключить возможности совпадения min_datetime и новой полученной даты
    min_datetime += timedelta(seconds=1)

    delta_seconds = int((max_datetime - min_datetime).total_seconds())
    if delta_seconds <= 0:
        random_seconds = 0
    else:
        random_seconds = random.randrange(delta_seconds)

    return min_datetime + timedelta(seconds=random_seconds)

def iter_month(year, month):
    d = datetime(year, month, 1)
    while d.month == month:
        yield d
        d += timedelta(days=1)

def _generate_users(ti, year: int, month: int) -> list[dict]:
    """
    Фунция для генерации пользователей
    """

    users = []
    k = 0

    for i in iter_month(year, month):
        for j in range(random.randint(7, 20)):
            adj = random.choice(ADJECTIVES)
            noun = random.choice(NOUNS)
            suffix = uuid.uuid4().hex[:4]
            domen = random.choice(DOMENS) + '.' + random.choice(CHARACTERS) + random.choice(CHARACTERS)

            # Подобная форма user_id будет гарантировать уникальность для 1000 пользователей
            users.append({'user_id': ((year % 100) * 100 + month) * 1000 + k,
                            'user_name': f"{adj}_{noun}_{suffix}",
                            'email': f"{adj}_{noun}_{suffix}@{domen}",
                            'registered_at': random_datetime_in_month(year, month)})
            
            k += 1
        
    ti.xcom_push(key="generated_users", value=users)

    return users

def _generate_logs(ti, year: int, month: int):
    """
    Функция для генерации логов
    """
    user_login_logout = dict()

    users = ti.xcom_pull(task_ids="generated_users", key="generated_users")

    # Генерирую интервалы входа-выхода в аккаунт для пользователей

    # Количество обязательных интервалов минимум по 5 в день
    for i in range(len(users)):
        user = users[i]
        
        login_t = random_datetime_in_day(user['registered_at'])
        logout_t = random_datetime_in_day(login_t)

        user_login_logout[i] = [(login_t, logout_t)]

    # Количество случайных интервалов
    number_of_login_logout = random.randint(200, 300)

    for i in range(number_of_login_logout):
        user_indx = random.randint(0, len(users) - 1)
        user = users[user_indx]

        max_logout = user_login_logout[user_indx][0][1]
        for i in user_login_logout[user_indx]:
            max_logout = max(max_logout, i[1])

        login_t = random_datetime_in_day(max_logout)
        logout_t = random_datetime_in_day(login_t)

        user_login_logout[user_indx].append((login_t, logout_t))

    topics = []
    k = 0
    # генерирую обязательную часть создания и удаления тем
    for i in range(len(users)):
        user = users[i]
        login_t, logout_t = random.choice(user_login_logout[i])
        created_at = random_datetime_in_interval(login_t, logout_t)
        topics.append({'topic_id': ((year % 100) * 100 + month) * 1000 + k,
                        'author_id': user['user_id'],
                        'title': random.choices(CHARACTERS, k=random.randint(1, 20)),
                        'description': '',
                        'created_at': created_at,
                        'deleted_at': random_datetime_in_interval(created_at, logout_t)})
        k += 1

    # случайно генерирую темы с учетом интервалов входа и выхода пользователей
    number_of_new_topics = random.randint(100, 200)

    for i in range(number_of_new_topics):
        user_indx = random.randint(0, len(users) - 1)
        user = users[user_indx]

        # Подобная форма topic_id будет гарантировать уникальность для 1000 тем
        topics.append({'topic_id': ((year % 100) * 100 + month) * 1000 + k,
                      'author_id': user['user_id'],
                      'title': random.choices(CHARACTERS, k=random.randint(1, 20)),
                      'description': '',
                      'created_at': random_datetime_in_interval(*random.choice(user_login_logout[user_indx])),
                      'deleted_at': None})
        k += 1


    messages = []
    k = 0
    # Генерирую сообщения от анонимных пользователей
    for i in range(len(topics)):
        messages.append({
            'message_id': ((year % 100) * 100 + month) * 10000 + k,
            'topic_id': topics[i]['topic_id'],
            'author_id': None,
            'body': random.choices(CHARACTERS, k=random.randint(1, 20)),
            'created_at': random_datetime_in_day(topics[i]['created_at'])
                                if topics[i]['deleted_at'] is None 
                                else random_datetime_in_interval(topics[i]['created_at'], topics[i]['deleted_at'])
        })
        k += 1

    for i in range(len(users)):
        user = users[i]
        login_t, logout_t = None, None

        # для ограничения интервала времени по созданию сообщения
        for j in user_login_logout[i]:
            login_t, logout_t = j
            if login_t <= topics[i]['created_at'] <= logout_t:
                break
        
        messages.append({
            'message_id': ((year % 100) * 100 + month) * 10000 + k,
            'topic_id': topics[i]['topic_id'],
            'author_id': user['user_id'],
            'body': random.choices(CHARACTERS, k=random.randint(1, 20)),
            'created_at': random_datetime_in_interval(topics[i]['created_at'], logout_t)
                                if topics[i]['deleted_at'] is None 
                                else random_datetime_in_interval(topics[i]['created_at'], topics[i]['deleted_at'])
        })
        k += 1

    # Создаю общий список логов
    forum_logs = []
    k = 0

    # логи регистрации
    for i in users:
        forum_logs.append({
            'log_id': ((year % 100) * 100 + month) * 10000 + k,
            'event_ts': i['registered_at'],
            'user_id': i['user_id'],
            'action_type_id': action_types['REGISTER'],
            'object_id': None, 
            'object_type': None,
            'response_status': 'success',
            'error_code': None
        })
        k += 1

    # логи входов\выходов и первого захода
    for i in range(len(user_login_logout)):
        min_login_t = user_login_logout[i][0][0]

        for login_t, logout_t in user_login_logout[i]:
            min_login_t = min(login_t, min_login_t)

            forum_logs.append({
                'log_id': ((year % 100) * 100 + month) * 10000 + k,
                'event_ts': login_t,
                'user_id': users[i]['user_id'],
                'action_type_id': action_types['LOGIN'],
                'object_id': None, 
                'object_type': None,
                'response_status': 'success',
                'error_code': None
            })
            k += 1

            forum_logs.append({
                'log_id': ((year % 100) * 100 + month) * 10000 + k,
                'event_ts': logout_t,
                'user_id': users[i]['user_id'],
                'action_type_id': action_types['LOGOUT'],
                'object_id': None, 
                'object_type': None,
                'response_status': 'success',
                'error_code': None
            })
            k += 1

        forum_logs.append({
            'log_id': ((year % 100) * 100 + month) * 10000 + k,
            'event_ts': min_login_t,
            'user_id': users[i]['user_id'],
            'action_type_id': action_types['FIRST_VISIT'],
            'object_id': None, 
            'object_type': None,
            'response_status': 'success',
            'error_code': None
        })
        k += 1


    # Логи создания\удаления темы
    for i in topics:
        forum_logs.append({
            'log_id': ((year % 100) * 100 + month) * 10000 + k,
            'event_ts': i['created_at'],
            'user_id': i['author_id'],
            'action_type_id': action_types['CREATE_TOPIC'],
            'object_id': i['topic_id'],
            'object_type': 'topic',
            'response_status': 'success',
            'error_code': None
        })
        k += 1

        if i['deleted_at'] is not None:
            forum_logs.append({
                'log_id': ((year % 100) * 100 + month) * 10000 + k,
                'event_ts': i['deleted_at'],
                'user_id': i['author_id'],
                'action_type_id': action_types['DELETE_TOPIC'],
                'object_id': i['topic_id'],
                'object_type': 'topic',
                'response_status': 'success',
                'error_code': None
            })
            k += 1

    # Ошибочные логи создания темы неавторизованными пользователями 
    for i in iter_month(year, month):
        for j in range(random.randint(2, 10)):
            forum_logs.append({
                'log_id': ((year % 100) * 100 + month) * 10000 + k,
                'event_ts': random_datetime_in_day(i),
                'user_id': None,
                'action_type_id': action_types['CREATE_TOPIC'],
                'object_id': None,
                'object_type': None,
                'response_status': 'error',
                'error_code': 'not_authenticated'
            })
            k += 1

    # Логи сообщений
    for i in messages:
        forum_logs.append({
            'log_id': ((year % 100) * 100 + month) * 10000 + k,
            'event_ts': i['created_at'],
            'user_id': i['author_id'],
            'action_type_id': action_types['CREATE_MESSAGE'],
            'object_id': i['message_id'],
            'object_type': 'message',
            'response_status': 'success',
            'error_code': None
        })
        k += 1

    # Логи заходов на темы, для упрощения сделал только по неавторизованным пользователям
    for i in topics:
        for j in range(random.randint(2, 4)):
            forum_logs.append({
                'log_id': ((year % 100) * 100 + month) * 10000 + k,
                'event_ts': random_datetime_in_day(i['created_at'])
                                if i['deleted_at'] is None 
                                else random_datetime_in_interval(i['created_at'], i['deleted_at']),
                'user_id': None,
                'action_type_id': action_types['ENTRY_TOPIC'],
                'object_id': i['topic_id'],
                'object_type': 'topic',
                'response_status': 'success',
                'error_code': None
            })
            k += 1

    ti.xcom_push(key="my_logs", value=forum_logs)
    ti.xcom_push(key="my_topics", value=topics)
    ti.xcom_push(key="my_messages", value=messages)
    ti.xcom_push(key="my_users", value=users)

    return forum_logs


def _validate_generated_data(ti):
    """
    Функция для проверки сгенерированных логов
    """
    logs_df = pd.DataFrame(ti.xcom_pull(task_ids="generate_logs_task", key="my_logs"))

    schema_logs = pa.DataFrameSchema(
        {
            "log_id": pa.Column(int),
            "event_ts": pa.Column("datetime64[ns]"),
            "user_id": pa.Column(float, nullable=True),
            "action_type_id": pa.Column(int, pa.Check.isin(list(action_types.values()))),
            "object_id": pa.Column(float, nullable=True),
            "object_type": pa.Column(str, 
                pa.Check(
                    lambda s: s.isin(["topic", "message"]) | s.isna(),
                    # Эта проверка явно разрешает либо значение из списка, либо None/NaN
                    error=f"Значение  должно быть 'topic', 'message' или None"), 
                nullable=True),
            "response_status": pa.Column(str, pa.Check.isin(['success', 'error'])),
            "error_code": pa.Column(str, nullable=True),
        }
    )

    schema_logs.validate(logs_df)

    logs_df_sorted = logs_df.loc[logs_df['action_type_id'].isin([action_types['LOGIN'], action_types['LOGOUT']])] \
                    .sort_values(["user_id", "event_ts"]) \
                    .reset_index() \
                    .copy()

    for i in range(1, len(logs_df_sorted)):
        if logs_df_sorted.iloc[i]['action_type_id'] == action_types['LOGIN'] and logs_df_sorted.iloc[i - 1]['action_type_id'] != action_types['LOGOUT']:
            raise pa.errors.SchemaError(
                schema=None,  # или ваша схема
                data=logs_df_sorted,
                message="был совершен вход, но сесия начавшаяся ранее, не была при этом завершена"
            )
        if logs_df_sorted.iloc[i]['action_type_id'] == action_types['LOGOUT'] and logs_df_sorted.iloc[i - 1]['action_type_id'] != action_types['LOGIN']:
            raise pa.errors.SchemaError(
                schema=None,  # или ваша схема
                data=logs_df_sorted,
                message="был совершен выход, но при этом ранее не было совершено входа"
            )

    if len(logs_df.loc[(logs_df['action_type_id'] == action_types['CREATE_TOPIC']) &
                       (logs_df['response_status'] != 'error') &
                       (logs_df['user_id'].isna())]) > 0:
        raise pa.errors.SchemaError(
            schema=None,  # или ваша схема
            data=logs_df.loc[(logs_df['action_type_id'] == action_types['CREATE_TOPIC']) &
                                    (logs_df['user_id'].isna())],
            message="Существуют логи, где тему создал неавторизованный пользователь"
        )

    return True

    

def _load_to_postgres(ti, year, month):
    """
    Функция для загрузки данных в postgres
    """
    logs = ti.xcom_pull(task_ids="generate_logs_task", key="my_logs")
    topics = ti.xcom_pull(task_ids="generate_logs_task", key="my_topics")
    messages = ti.xcom_pull(task_ids="generate_logs_task", key="my_messages")
    users = ti.xcom_pull(task_ids="generate_logs_task", key="my_users")

    if not users:
        print("Нет логов приложения для загрузки.")
        return

    pg_hook = PostgresHook(postgres_conn_id="my_postgres")

    sql = f"""
        CREATE TABLE IF NOT EXISTS forum_logs_{year}_{month}
        PARTITION OF forum_logs
        FOR VALUES FROM ('{date(year, month, 1)}') 
        TO ('{(date(year, month, 1) + timedelta(days=31)).replace(day=1)}');
    """
    
    pg_hook.run(sql)

    conn = pg_hook.get_conn()
    cur = conn.cursor()

    try:
        cur.executemany(
            """
            INSERT INTO users (user_id, user_name, email, registered_at)
            VALUES (%(user_id)s, %(user_name)s, %(email)s, %(registered_at)s);
            """,
            users,
        )

        cur.executemany(
            """
            INSERT INTO topics (topic_id, author_id, title, description, created_at, deleted_at)
            VALUES (%(topic_id)s, %(author_id)s, %(title)s, %(description)s, %(created_at)s, %(deleted_at)s);
            """,
            topics,
        )

        cur.executemany(
            """
            INSERT INTO messages (message_id, topic_id, author_id, body, created_at)
            VALUES (%(message_id)s, %(topic_id)s, %(author_id)s, %(body)s, %(created_at)s);
            """,
            messages,
        )

        cur.executemany(
            """
            INSERT INTO forum_logs (log_id, event_ts, user_id, action_type_id, object_id, object_type, response_status, error_code)
            VALUES (%(log_id)s, %(event_ts)s, %(user_id)s, %(action_type_id)s, %(object_id)s, %(object_type)s, %(response_status)s, %(error_code)s);
            """,
            logs,
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()



with DAG(
    dag_id="logs_generation_and_upload_operators",
    start_date=datetime(2024, 1, 1),
    schedule=None,          # запуск вручную; замените на cron при необходимости
    catchup=False,
    tags=["logs", "postgres"],
) as dag:

    generate_users = PythonOperator(
        task_id="generated_users",
        python_callable=_generate_users,
        op_kwargs={'year': 2026, 'month': 3} 
    )

    generate_logs = PythonOperator(
        task_id="generate_logs_task",
        python_callable=_generate_logs,
        op_kwargs={'year': 2026, 'month': 3} 
    )

    validate_generated_data = PythonOperator(
        task_id="validate_logs",
        python_callable=_validate_generated_data
    )

    load_logs = PythonOperator(
        task_id="load_logs",
        python_callable=_load_to_postgres,
        op_kwargs={'year': 2026, 'month': 3} 
    )

    # Явно выстраиваем зависимости.
    # Обе задачи загрузки зависят от генерации и могут выполняться параллельно.
    generate_users >> generate_logs >> validate_generated_data >> load_logs
