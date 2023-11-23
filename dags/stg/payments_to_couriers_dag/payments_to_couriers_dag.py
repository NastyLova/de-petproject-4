import logging

from airflow.decorators import dag, task
from stg.payments_to_couriers_dag.payments_to_couriers_lib import delivery, couriers
from lib import ConnectionBuilder
from airflow.hooks.base import BaseHook
import pendulum
from datetime import datetime

log = logging.getLogger(__name__)

cohort_params = BaseHook.get_connection("cohort_params")
nickname = cohort_params.extra_dejson['nickname']
cohort = cohort_params.extra_dejson['cohort']
api_token = cohort_params.extra_dejson['api_token']
sort_field = '_id'
sort_direction = 'asc'
from_ = datetime(2023, 7, 30)
headers = {
    "X-API-KEY": api_token,
    "X-Nickname": nickname,
    "X-Cohort": cohort
}

@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'project'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def stg_payments_to_courier_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="load_delivery")
    def load_deliveries():
        # создаем экземпляр класса, в котором реализована логика.
        d = delivery(headers, sort_field, sort_direction, log, dwh_pg_connect, from_, 10)
        d.load_deliveries()

    # Инициализируем объявленные таски.
    load_delivery = load_deliveries()

    @task(task_id="load_courier")
    def load_couriers():
        # создаем экземпляр класса, в котором реализована логика.
        c = couriers(headers, sort_field, sort_direction, log, dwh_pg_connect)
        c.load_couriers()

    # Инициализируем объявленные таски.
    load_courier = load_couriers()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    [load_courier, load_delivery]


stg_payments_to_courier_dag = stg_payments_to_courier_dag()