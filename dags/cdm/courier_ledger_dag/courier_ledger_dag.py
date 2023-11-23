import logging

import pendulum
from airflow.decorators import dag, task
from cdm.courier_ledger_dag.courier_ledger_lib import CLLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds', 'stg'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def cdm_courier_leger_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="cl_load")
    def load_cl():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = CLLoader(dwh_pg_connect, log)
        rest_loader.load_cls()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    courier_leger = load_cl()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    courier_leger


cdm_courier_leger_dag = cdm_courier_leger_dag()
