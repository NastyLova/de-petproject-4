CREATE TABLE dds.dm_deliveries (
	id serial4 NOT NULL,
	delivery_id varchar NOT NULL,
	delivery_ts timestamp NOT NULL,
	rate int4 NOT NULL,
	sum numeric(14, 2) NOT NULL,
	tip_sum numeric(14, 2) NOT NULL,
	address text NOT NULL,
	CONSTRAINT dm_deliveries_check_rate CHECK (((rate >= 1) AND (rate <= 5))),
	CONSTRAINT dm_deliveries_check_sum CHECK ((sum >= (0)::numeric)),
	CONSTRAINT dm_deliveries_check_tip_sum CHECK ((tip_sum >= (0)::numeric)),
	CONSTRAINT dm_deliveries_pk PRIMARY KEY (id),
	CONSTRAINT dm_deliveries_un UNIQUE (delivery_id)
);

CREATE TABLE dds.dm_couriers (
	id serial4 NOT NULL,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	CONSTRAINT dm_couriers_pk PRIMARY KEY (id),
	CONSTRAINT dm_couriers_un UNIQUE (courier_id)
);

