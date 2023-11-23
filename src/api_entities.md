1. Список полей, которые необходимы для витрины:
	id — идентификатор записи.
	courier_id — ID курьера, которому перечисляем.
	courier_name — Ф. И. О. курьера.
	settlement_year — год отчёта.
	settlement_month — месяц отчёта, где 1 — январь и 12 — декабрь.
	orders_count — количество заказов за период (месяц).
	orders_total_sum — общая стоимость заказов.
	rate_avg — средний рейтинг курьера по оценкам пользователей.
	order_processing_fee — сумма, удержанная компанией за обработку заказов, которая высчитывается как orders_total_sum * 0.25.
	courier_order_sum — сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы. За каждый доставленный заказ курьер должен получить некоторую сумму в зависимости от рейтинга (см. ниже).
		r < 4 — 5% от заказа, но не менее 100 р.;
		4 <= r < 4.5 — 7% от заказа, но не менее 150 р.;
		4.5 <= r < 4.9 — 8% от заказа, но не менее 175 р.;
		4.9 <= r — 10% от заказа, но не менее 200 р.
	courier_tips_sum — сумма, которую пользователи оставили курьеру в качестве чаевых.
	courier_reward_sum — сумма, которую необходимо перечислить курьеру. Вычисляется как courier_order_sum + courier_tips_sum * 0.95 (5% — комиссия за обработку платежа).
2. Список таблиц в слое DDS, из которых возьмем поля для витрины.
	id — автогенерация при заполнении
	courier_id — вытягиваем по API GET/couriers
	courier_name — вытягиваем по API GET/couriers
	settlement_year — dds.dm_timestamps - есть в хранилище
	settlement_month —  ddsdm_timestamps - есть в хранилище
	orders_count — dds.dm_orders - есть в хранилище
	orders_total_sum — dds.dm_orders - есть в хранилище
	rate_avg — вытягиваем по API GET/deliveries
	order_processing_fee — dds.dm_orders - есть в хранилище
	courier_order_sum — dds.dm_orders - есть в хранилище
	courier_tips_sum — вытягиваем по API GET/deliveries
	courier_reward_sum — берем уже имеющиеся атрибуты
	
	Создать таблицы:
	dds.fct_orders_delivery
		id
		delivery_id
		order_id
		courier_id
		
	dds.dm_deliveries
		id
		delivery_id
		delivery_ts
		rate
		sum
		tip_sum
		
	dds.dm_couriers
		id
		courier_id
		courier_name	
	
3. Список сущностей и полей, которые необходимо загрузить из API:
	stg.deliverysystem_deliveries - GET/deliveries
		id serial4
		request_id varchar
		request_date timestamp
		response text


	stg.deliverysystem_couriers - GET/couriers
		id serial4
		request_id varchar
		request_date timestamp
		response text
