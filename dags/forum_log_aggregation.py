from airflow import DAG
from datetime import date, datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.models.param import Param
import pandera as pa
import pandas as pd 

import random
import uuid
import string
import logging

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

def _extract_raw_logs(ti, start_date: str, end_date: str):
    """
    Функция для получения сырых логов из БД
    """

    try:
        hook = PostgresHook(postgres_conn_id="my_postgres")
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(
            f"""
            SELECT 
                * 
            FROM 
                forum_logs
            where 
                event_ts::date >= '{ start_date }'::date - interval '1 day'
                AND event_ts::date <= '{ end_date }'::date
            """
        )

        logs = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
       
    finally:
        cursor.close()
        conn.close()

    ti.xcom_push(key="raw_logs", value=logs)

    return logs

def _validate_data(ti):
    """
    Функция для валидации сырых данных
    """

    logs_df = ti.xcom_pull(task_ids="extract_raw_logs", key="raw_logs")

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

    return True

def _transform_aggregates(ti):
    """
    Функция обрабатывает полученные данные
    """
    logs_df = ti.xcom_pull(task_ids="extract_raw_logs", key="raw_logs")

    new_accounts = logs_df.loc[logs_df['action_type_id'] == action_types['REGISTER'], 'user_id'].groupby(logs_df['event_ts'].dt.normalize()).count()

    total_messages = logs_df.loc[logs_df['action_type_id'] == action_types['CREATE_MESSAGE'], 'user_id'].groupby(logs_df['event_ts'].dt.normalize()).size()
    
    anon_messages_pct = (logs_df.loc[(logs_df['action_type_id'] == action_types['CREATE_MESSAGE']) 
                                    & (logs_df['user_id'].isna()), 'user_id'].groupby(logs_df['event_ts'].dt.normalize()).size() 
                                    / total_messages * 100)

    login = logs_df.loc[logs_df['action_type_id'] == action_types['LOGIN'], ["user_id", "event_ts"]].sort_values(["user_id", "event_ts"]).reset_index()
    logout = logs_df.loc[logs_df['action_type_id'] == action_types['LOGOUT'], ["user_id", "event_ts"]].sort_values(["user_id", "event_ts"]).reset_index()
    login['diff'] = logout['event_ts'] - login['event_ts']
    login['diff'] = login['diff'].dt.total_seconds()
    avg_session_duration_sec = login['diff'].groupby(login['event_ts'].dt.normalize()).mean()

    tmp = logs_df.loc[(logs_df['action_type_id'] == action_types['CREATE_TOPIC']) 
                      & (logs_df['response_status'] == 'success'), 'user_id'].groupby(logs_df['event_ts'].dt.normalize()).count()

    topics_growth_pct = (tmp - tmp.shift(1)) / tmp.shift(1) * 100

    tmp = logs_df.copy()
    tmp['event_ts'] = tmp['event_ts'].dt.normalize() # просто дата без времени

    reg = (tmp.loc[tmp['action_type_id'] == action_types['REGISTER']]
       .groupby('user_id')['event_ts']
       .min()
       .rename('reg_date'))

    logins = (tmp.loc[tmp['action_type_id'] == action_types['LOGIN']]
          .drop_duplicates(['user_id', 'event_ts']))
    
    logins = logins.merge(reg, on='user_id')

    logins['is_d1'] = (logins['event_ts'] == logins['reg_date'] + pd.Timedelta(days=1))

    d1 = logins.loc[logins['is_d1'], 'user_id'].groupby(logins['event_ts']).count()

    reg_count = tmp.loc[tmp['action_type_id'] == action_types['REGISTER'], 'user_id'].groupby(tmp['event_ts']).count()

    d1_retention_pct = d1 / reg_count.shift(1) * 100

    aggregate_logs = pd.DataFrame({'new_accounts': new_accounts, 
                    'total_messages': total_messages, 
                    'anon_messages_pct': anon_messages_pct,
                    'avg_session_duration_sec': avg_session_duration_sec,
                    'topics_growth_pct': topics_growth_pct,
                    'd1_retention_pct': d1_retention_pct})
    
    aggregate_logs = aggregate_logs.drop(aggregate_logs.index.min())

    ti.xcom_push(key="aggregate_logs", value=aggregate_logs)

    return aggregate_logs

def _export_to_csv(ti):
    aggregate_logs = ti.xcom_pull(task_ids="transform_aggregates", key="aggregate_logs")

    aggregate_logs.to_csv('/opt/airflow/data/aggregate_logs.csv')

    return True

def _alert_on_anomaly(ti):
    data = pd.read_csv('/opt/airflow/data/aggregate_logs.csv')

    for i in range(len(data)):
        if data.iloc[i]['new_accounts'] == 0:
            logging.warning(
                f"Показатель new_accounts равен 0. Не было зарегистрировано новых пользователей в день : {data.iloc[i]['event_ts']}."
            )

        if data.iloc[i]['total_messages'] < 6:
            logging.warning(
                f"Было создано мало сообщений в день: {data.iloc[i]['event_ts']}"
            )

        if data.iloc[i]['anon_messages_pct'] > 80:
            logging.warning(
                f"Большую часть сообщений написали анонимные пользователи в день: {data.iloc[i]['event_ts']}"
            )

        if data.iloc[i]['topics_growth_pct'] > 100:
            logging.warning(
                f"Количество тем увеличилось более чем в 2 раза в день: {data.iloc[i]['event_ts']}"
            )

        if data.iloc[i]['topics_growth_pct'] < -50:
            logging.warning(
                f"Количество тем уменьшилось на 50% в день: {data.iloc[i]['event_ts']}"
            )

        if data.iloc[i]['d1_retention_pct'] == 0:
            logging.warning(
                f"Ни один пользователь не залогинился на след день после регистрации в день: {data.iloc[i]['event_ts']}"
            )

    return True

with DAG(
    dag_id="forum_log_aggregation",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    params={
        "start_date": Param(None, type=["null", "string"], description="YYYY-MM-DD"),
        "end_date": Param(None, type=["null", "string"], description="YYYY-MM-DD"),
    },
) as dag:

    check_data_availability = SqlSensor(
        task_id="check_data_availability",
        conn_id="my_postgres",
        sql="""
            SELECT COUNT(*) 
            FROM forum_logs 
            WHERE 
                event_ts::date >= '{{ params.start_date or macros.ds_add(ds, -1) }}'::date - interval '1 day'
                AND event_ts::date <= '{{ params.end_date or macros.ds_add(ds, -1) }}'::date
        """,
        poke_interval=30,        # Проверять каждые 5 минут
        timeout=60 * 60 * 12,     # Ждать максимум 12 часов
        mode="reschedule",        # Не занимать слот воркера постоянно
    )

    extract_raw_logs = PythonOperator(
        task_id="extract_raw_logs",
        python_callable=_extract_raw_logs,
        op_kwargs={'start_date': '{{ params.start_date or macros.ds_add(ds, -1) }}',
                    'end_date': '{{ params.end_date or macros.ds_add(ds, -1) }}'}
    )

    validate_data = PythonOperator(
        task_id="validate_data",
        python_callable=_validate_data
    )

    transform_aggregates = PythonOperator(
        task_id="transform_aggregates",
        python_callable=_transform_aggregates
    )

    export_to_csv = PythonOperator(
        task_id="export_to_csv",
        python_callable=_export_to_csv
    )

    alert_on_anomaly = PythonOperator(
        task_id="alert_on_anomaly",
        python_callable=_alert_on_anomaly
    )


    check_data_availability >> extract_raw_logs >> validate_data >> transform_aggregates >> export_to_csv >> alert_on_anomaly