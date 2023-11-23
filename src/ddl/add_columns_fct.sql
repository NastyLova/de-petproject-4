ALTER TABLE dds.fct_product_sales ADD delivery_id bigint NULL;
ALTER TABLE dds.fct_product_sales ADD courier_id bigint NULL;
ALTER TABLE dds.fct_product_sales ADD CONSTRAINT fct_product_sales_courier_id_fk FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id);
ALTER TABLE dds.fct_product_sales ADD CONSTRAINT fct_product_sales_delivery_id_fk FOREIGN KEY (delivery_id) REFERENCES dds.dm_deliveries(id);

update dds.fct_product_sales fs 
set courier_id = t2.courier_id, delivery_id = t2.delivery_id
from (
select
	dd.id delivery_id,
	dc.id courier_id,
	do2.id order_id
from
	(
	select  json_array_elements(dd.response::JSON)::JSON->>'courier_id' as courier_id,
			json_array_elements(dd.response::JSON)::JSON->>'order_id' as order_id,
			json_array_elements(dd.response::JSON)::JSON->>'delivery_id' as delivery_id
	from stg.deliverysystem_deliveries dd ) t
inner join dds.dm_orders do2 on do2.order_key = t.order_id
inner join dds.dm_couriers dc on dc.courier_id = t.courier_id
inner join dds.dm_deliveries dd on dd.delivery_id = t.delivery_id) t2
where fs.order_id = t2.order_id;