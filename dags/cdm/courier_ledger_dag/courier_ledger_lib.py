from logging import Logger
from typing import List

from cdm.cdm_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime, date, time

class CourierLedgertObj(BaseModel):
    courier_id: int
    courier_name: str
    settlement_year: int
    settlement_month: int
    orders_count: int
    orders_total_sum: float
    rate_avg: float
    order_processing_fee: float
    courier_order_sum: float
    courier_tips_sum: float
    courier_reward_sum: float
    class Config:
        orm_mode = True


class CLOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_cl(self) -> List[CourierLedgertObj]:
        with self._db.client().cursor(row_factory=class_row(CourierLedgertObj)) as cur:
            cur.execute(
                """
                    with fact as (select fps.courier_id, 
                            dc.courier_name ,
                            dt."year" as settlement_year,
                            dt."month" as settlement_month,
                            count(distinct fps.order_id) as orders_count,
                            sum(fps.total_sum) as orders_total_sum,
                            avg(dd.rate) as rate_avg,
                            case when avg(dd.rate) < 4 then greatest(sum(fps.total_sum) * 0.05, 100)
                                when avg(dd.rate) >= 4 and avg(dd.rate) < 4.5 then greatest(sum(fps.total_sum) * 0.07, 150)
                                when avg(dd.rate) >= 4.5 and avg(dd.rate) < 4.9 then greatest(sum(fps.total_sum) * 0.08, 175)
                                when avg(dd.rate) >= 4.9 then greatest(sum(fps.total_sum) * 0.1, 200) end courier_order_sum,
                            sum(dd.tip_sum) as courier_tips_sum
                    from dds.fct_product_sales fps 
                    inner join dds.dm_couriers dc on dc.id = fps.courier_id 
                    inner join dds.dm_orders do2 on do2.id = fps.order_id 
                    inner join dds.dm_deliveries dd on dd.id = fps.delivery_id 
                    inner join dds.dm_timestamps dt on dt.id  = do2.timestamp_id
                    group by fps.courier_id, 
                            dc.courier_name,
                            settlement_year,
                            settlement_month)
                            
                    select courier_id, 
                            courier_name,
                            settlement_year,
                            settlement_month,
                            orders_count,
                            orders_total_sum,
                            rate_avg,
                            orders_total_sum * 0.25 as order_processing_fee,
                            courier_order_sum,
                            courier_tips_sum,
                            courier_order_sum + courier_tips_sum * 0.95 as courier_reward_sum
                    from fact; 
                """
            )
            objs = cur.fetchall()
        return objs


class CLDestRepository:
    def insert_cl(self, conn: Connection, cl: CourierLedgertObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.dm_courier_ledger (courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
                    VALUES (%(courier_id)s, %(courier_name)s, %(settlement_year)s, %(settlement_month)s, %(orders_count)s, %(orders_total_sum)s, %(rate_avg)s, %(order_processing_fee)s, %(courier_order_sum)s, %(courier_tips_sum)s, %(courier_reward_sum)s)
                    ON CONFLICT (courier_id, courier_name, settlement_year, settlement_month) DO UPDATE
                    SET 
                        orders_count = EXCLUDED.orders_count,
                        orders_total_sum = EXCLUDED.orders_total_sum,
                        rate_avg = EXCLUDED.rate_avg,
                        order_processing_fee = EXCLUDED.order_processing_fee,
                        courier_order_sum = EXCLUDED.courier_order_sum,
                        courier_tips_sum = EXCLUDED.courier_tips_sum,
                        courier_reward_sum = EXCLUDED.courier_reward_sum;
                """,
                {
                    "courier_id": cl.courier_id,
                    "courier_name": cl.courier_name,
                    "settlement_year": cl.settlement_year,
                    "settlement_month": cl.settlement_month,
                    "orders_count": cl.orders_count,
                    "orders_total_sum": cl.orders_total_sum,
                    "rate_avg": cl.rate_avg,
                    "order_processing_fee": cl.order_processing_fee,
                    "courier_order_sum": cl.courier_order_sum,
                    "courier_tips_sum": cl.courier_tips_sum,
                    "courier_reward_sum": cl.courier_reward_sum
                },
            )


class CLLoader:
    WF_KEY = "dm_courier_ledger_cdm"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = CLOriginRepository(pg_dest)
        self.dds = CLDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_cls(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:
            # Вычитываем очередную пачку объектов.
            load_queue = self.origin.list_cl()
            self.log.info(f"Found {len(load_queue)} users to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for sr in load_queue:
                self.dds.insert_cl(conn, sr)

            self.log.info(f"Load finished")