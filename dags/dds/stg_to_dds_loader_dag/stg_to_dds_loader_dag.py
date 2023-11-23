import logging

import pendulum
from airflow.decorators import dag, task
from dds.stg_to_dds_loader_dag.stg_to_dds_loader_lib import (UserLoader, 
                                                             RestaurantLoader, 
                                                             TimestampLoader, 
                                                             ProductLoader, 
                                                             OrderLoader, 
                                                             SaleLoader, 
                                                             DeliveryLoader,
                                                             CourierLoader)
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds', 'stg'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def stg_to_dds_loader_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="users_load")
    def load_users():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = UserLoader(dwh_pg_connect, log)
        rest_loader.load_users()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    users = load_users()

    @task(task_id="restaurants_load")
    def load_restaurants():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = RestaurantLoader(dwh_pg_connect, log)
        rest_loader.load_restaurants()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    restaurants = load_restaurants()

    @task(task_id="timestamps_load")
    def load_timestamps():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = TimestampLoader(dwh_pg_connect, log)
        rest_loader.load_timestamps()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    timestamps = load_timestamps()

    @task(task_id="products_load")
    def load_products():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = ProductLoader(dwh_pg_connect, log)
        rest_loader.load_products()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    products = load_products()

    @task(task_id="orders_load")
    def load_orders():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = OrderLoader(dwh_pg_connect, log)
        rest_loader.load_orders()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    orders = load_orders()

    @task(task_id="sales_load")
    def load_sales():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = SaleLoader(dwh_pg_connect, log)
        rest_loader.load_sales()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    sales = load_sales()

    @task(task_id="deliveries_load")
    def load_deliveries():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DeliveryLoader(dwh_pg_connect, log)
        rest_loader.load_deliveries()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    deliveries = load_deliveries()

    @task(task_id="couriers_load")
    def load_couriers():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = CourierLoader(dwh_pg_connect, log)
        rest_loader.load_couriers()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    couriers = load_couriers()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    [users, restaurants, timestamps, deliveries, couriers] >> products >> orders >> sales


stg_to_dds_loader_dag = stg_to_dds_loader_dag()
