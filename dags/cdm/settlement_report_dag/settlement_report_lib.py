from logging import Logger
from typing import List

from cdm.cdm_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime, date, time

class SettlementReportObj(BaseModel):
    restaurant_id: int
    restaurant_name: str
    settlement_date: date
    orders_count: int
    orders_total_sum: float
    orders_bonus_payment_sum: float
    orders_bonus_granted_sum: float
    order_processing_fee: float
    restaurant_reward_sum: float
    class Config:
        orm_mode = True


class SettlementReportOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_sr(self, limit: int) -> List[SettlementReportObj]:
        with self._db.client().cursor(row_factory=class_row(SettlementReportObj)) as cur:
            cur.execute(
                """
                    select  dr.id as restaurant_id,
                            dr.restaurant_name,
                            dt.date as settlement_date,
                            count(distinct fps.order_id) as orders_count,
                            sum(fps.total_sum) as orders_total_sum,
                            sum(fps.bonus_payment) as orders_bonus_payment_sum,
                            sum(fps.bonus_grant) as orders_bonus_granted_sum,
                            sum(fps.total_sum) * 0.25 as order_processing_fee,
                            sum(fps.total_sum) - sum(fps.bonus_payment) - (sum(fps.total_sum) * 0.25) as restaurant_reward_sum
                    from dds.fct_product_sales fps 
                    inner join dds.dm_orders do2 on do2.id = fps.order_id 
                    inner join dds.dm_restaurants dr on dr.id  = do2.restaurant_id 
                    inner join dds.dm_timestamps dt on dt.id  = do2.timestamp_id 
                    where dt."date" between now()::date - interval '1 month' and now()
                    group by dr.id, dr.restaurant_name, dt.date
                    ORDER BY dr.id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class SRDestRepository:
    def insert_sr(self, conn: Connection, sr: SettlementReportObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.dm_settlement_report(restaurant_id, restaurant_name, settlement_date, orders_count, orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, %(settlement_date)s, %(orders_count)s, %(orders_total_sum)s, %(orders_bonus_payment_sum)s, %(orders_bonus_granted_sum)s, %(order_processing_fee)s, %(restaurant_reward_sum)s)
                    ON CONFLICT (settlement_date, restaurant_id, restaurant_name) DO UPDATE
                    SET
                        orders_count = EXCLUDED.orders_count,
                        orders_total_sum = EXCLUDED.orders_total_sum,
                        orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                        orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                        order_processing_fee = EXCLUDED.order_processing_fee,
                        restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
                """,
                {
                    "restaurant_id": sr.restaurant_id,
                    "restaurant_name": sr.restaurant_name,
                    "settlement_date": sr.settlement_date,
                    "orders_count": sr.orders_count,
                    "orders_total_sum": sr.orders_total_sum,
                    "orders_bonus_payment_sum": sr.orders_bonus_payment_sum,
                    "orders_bonus_granted_sum": sr.orders_bonus_granted_sum,
                    "order_processing_fee": sr.order_processing_fee,
                    "restaurant_reward_sum": sr.restaurant_reward_sum
                },
            )


class SRLoader:
    WF_KEY = "dm_settlement_report_cdm"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = SettlementReportOriginRepository(pg_dest)
        self.dds = SRDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_users(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:
            # Вычитываем очередную пачку объектов.
            load_queue = self.origin.list_sr(self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} users to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for sr in load_queue:
                self.dds.insert_sr(conn, sr)

            self.log.info(f"Load finished")