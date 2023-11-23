from airflow.hooks.base import BaseHook
from psycopg import Connection
from lib.dict_util import str2json, json2str
from stg.stg_settings_repository import EtlSetting, StgEtlSettingsRepository

from logging import Logger
import requests
from datetime import datetime

class delivery:
    def __init__(self, headers: dict, sort_field: str, sort_direction: str,log: Logger, pg_dest: Connection, date_from: datetime = datetime(2022, 1, 1), limit: int = 50) -> None:
        self.headers = headers
        self.sort_field = sort_field
        self.sort_direction = sort_direction
        self.log = log
        self.pg_dest = pg_dest
        self.date_from = date_from
        self.limit = limit
        self.settings_repository = StgEtlSettingsRepository()

    def insert_delivery(self, conn: Connection, request_id: str, request_date: datetime, response: dict) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_deliveries(request_id, request_date, response)
                    VALUES (%(request_id)s, %(request_date)s, %(response)s)
                    ON CONFLICT (response) DO UPDATE
                    SET
                        request_id = EXCLUDED.request_id,
                        request_date = EXCLUDED.request_date;
                """,
                {
                    "request_id": request_id,
                    "request_date": request_date,
                    "response": json2str(response)
                },
            )

    def load_deliveries(self):
        '''
        Загрузка данных о доставках.
        '''
        WF_KEY = "deliveries_origin_to_stg_workflow"
        LAST_LOADED_ID_KEY = "last_loaded_offset"
        api_conn = BaseHook.get_connection('get_delivery_data')
        api_endpoint = api_conn.host
        # Открываем транзацию и записываем пачками полученные jsons array
        with self.pg_dest.connection() as conn:
            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=WF_KEY, workflow_settings={LAST_LOADED_ID_KEY: 0})

            # Вычитываем очередную пачку объектов.
            workflow_settings = wf_setting.workflow_settings
            method_url = '/deliveries'
            limit = self.limit
            offset = workflow_settings[LAST_LOADED_ID_KEY]
            i = 1
            # Получаем пачками данные пока не получим пустой массив объектов
            while i > 0:
                # Формируем строку запроса и делаем запрос
                method_str = f'''https://{api_endpoint}{method_url}?sort_field={self.sort_field}&sort_direction={self.sort_direction}&limit={limit}&offset={offset}&from={self.date_from}'''
                r = requests.get(method_str, headers=self.headers)
                response_dict = str2json(r.content)
                
                i = len(response_dict)
                if i > 0:
                    # Записываем данные в базу, если что-то пришло
                    self.insert_delivery(conn, r.headers['x-request-id'], r.headers['date'], response_dict)
                    self.log.info(f'Loaded {i} rows')
                    offset += len(response_dict)
            self.log.info(f'Total loaded {offset} rows')

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            workflow_settings[LAST_LOADED_ID_KEY] = offset
            wf_setting_json = json2str(workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {workflow_settings}")

class couriers:
    def __init__(self, headers: dict, sort_field: str, sort_direction: str,log: Logger, pg_dest: Connection, limit: int = 50) -> None:
        self.headers = headers
        self.sort_field = sort_field
        self.sort_direction = sort_direction
        self.log = log
        self.pg_dest = pg_dest
        self.limit = limit
        self.settings_repository = StgEtlSettingsRepository()

    def insert_courier(self, conn: Connection, request_id: str, request_date: datetime, response: dict) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_couriers(request_id, request_date, response)
                    VALUES (%(request_id)s, %(request_date)s, %(response)s)
                    ON CONFLICT (response) DO UPDATE
                    SET
                        request_id = EXCLUDED.request_id,
                        request_date = EXCLUDED.request_date;
                """,
                {
                    "request_id": request_id,
                    "request_date": request_date,
                    "response": json2str(response)
                },
            )

    def load_couriers(self):
        '''
        Загрузка данных о курьерах.
        '''
        WF_KEY = "couriers_origin_to_stg_workflow"
        LAST_LOADED_ID_KEY = "last_loaded_offset"
        api_conn = BaseHook.get_connection('get_delivery_data')
        api_endpoint = api_conn.host

        with self.pg_dest.connection() as conn:
            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=WF_KEY, workflow_settings={LAST_LOADED_ID_KEY: 0})

            # Вычитываем очередную пачку объектов.
            workflow_settings = wf_setting.workflow_settings
            method_url = '/couriers'
            limit = self.limit
            offset = workflow_settings[LAST_LOADED_ID_KEY]
            i = 1
            # Получаем пачками данные пока не получим пустой массив объектов
            while i > 0:
                # Формируем строку запроса и делаем запрос
                method_str = f'''https://{api_endpoint}{method_url}?sort_field={self.sort_field}&sort_direction={self.sort_direction}&limit={limit}&offset={offset}'''
                r = requests.get(method_str, headers=self.headers)
                response_dict = str2json(r.content)
                
                i = len(response_dict)
                if i > 0:
                    # Записываем данные в базу, если что-то пришло
                    self.insert_courier(conn, r.headers['x-request-id'], r.headers['date'], response_dict)
                    self.log.info(f'Loaded {i} rows')
                    offset += len(response_dict)
            self.log.info(f'Total loaded {offset} rows')

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            workflow_settings[LAST_LOADED_ID_KEY] = offset
            wf_setting_json = json2str(workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {workflow_settings}")


