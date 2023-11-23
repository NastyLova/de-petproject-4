-- cdm.dm_settlement_report definition

-- Drop table

-- DROP TABLE cdm.dm_settlement_report;

CREATE TABLE cdm.dm_settlement_report (
	id serial4 NOT NULL,
	restaurant_id varchar NOT NULL,
	restaurant_name varchar NOT NULL,
	settlement_date date NOT NULL,
	orders_count int4 NOT NULL,
	orders_total_sum numeric(14, 2) NOT NULL DEFAULT 0,
	orders_bonus_payment_sum numeric(14, 2) NOT NULL DEFAULT 0,
	orders_bonus_granted_sum numeric(14, 2) NOT NULL DEFAULT 0,
	order_processing_fee numeric(14, 2) NOT NULL DEFAULT 0,
	restaurant_reward_sum numeric(14, 2) NOT NULL DEFAULT 0,
	CONSTRAINT dm_settlement_report_order_processing_fee_check CHECK ((order_processing_fee >= (0)::numeric)),
	CONSTRAINT dm_settlement_report_orders_bonus_granted_sum_check CHECK ((orders_bonus_granted_sum >= (0)::numeric)),
	CONSTRAINT dm_settlement_report_orders_bonus_payment_sum_check CHECK ((orders_bonus_payment_sum >= (0)::numeric)),
	CONSTRAINT dm_settlement_report_orders_count_check CHECK ((orders_count >= 0)),
	CONSTRAINT dm_settlement_report_orders_total_sum_check CHECK ((orders_total_sum >= (0)::numeric)),
	CONSTRAINT dm_settlement_report_pk PRIMARY KEY (id),
	CONSTRAINT dm_settlement_report_restaurant_reward_sum_check CHECK ((restaurant_reward_sum >= (0)::numeric)),
	CONSTRAINT dm_settlement_report_settlement_date_check CHECK (((date_part('year'::text, settlement_date) >= (2022)::double precision) AND (date_part('year'::text, settlement_date) < (2500)::double precision))),
	CONSTRAINT dm_settlement_report_un UNIQUE (restaurant_id, restaurant_name, settlement_date)
);

-- Permissions

ALTER TABLE cdm.dm_settlement_report OWNER TO jovyan;
GRANT ALL ON TABLE cdm.dm_settlement_report TO jovyan;

-- cdm.srv_wf_settings definition

-- Drop table

-- DROP TABLE cdm.srv_wf_settings;

CREATE TABLE cdm.srv_wf_settings (
	id serial4 NOT NULL,
	workflow_key varchar NOT NULL,
	workflow_settings json NOT NULL,
	CONSTRAINT srv_wf_settings_pk PRIMARY KEY (workflow_key)
);

-- Permissions

ALTER TABLE cdm.srv_wf_settings OWNER TO jovyan;
GRANT ALL ON TABLE cdm.srv_wf_settings TO jovyan;

-- dds.dm_restaurants definition

-- Drop table

-- DROP TABLE dds.dm_restaurants;

CREATE TABLE dds.dm_restaurants (
	id serial4 NOT NULL,
	restaurant_id varchar NOT NULL,
	restaurant_name varchar NOT NULL,
	active_from timestamp NOT NULL DEFAULT now(),
	active_to timestamp NOT NULL DEFAULT now(),
	CONSTRAINT dm_restaurants_pk PRIMARY KEY (id),
	CONSTRAINT dm_restaurants_un UNIQUE (restaurant_id)
);

-- Permissions

ALTER TABLE dds.dm_restaurants OWNER TO jovyan;
GRANT ALL ON TABLE dds.dm_restaurants TO jovyan;

-- dds.dm_timestamps definition

-- Drop table

-- DROP TABLE dds.dm_timestamps;

CREATE TABLE dds.dm_timestamps (
	id serial4 NOT NULL,
	ts timestamp NOT NULL,
	"year" int2 NOT NULL,
	"month" int2 NOT NULL,
	"day" int2 NOT NULL,
	"time" varchar NOT NULL,
	"date" date NOT NULL,
	CONSTRAINT dm_timestamps_day_check CHECK (((day >= 1) AND (day <= 31))),
	CONSTRAINT dm_timestamps_month_check CHECK (((month >= 1) AND (month <= 12))),
	CONSTRAINT dm_timestamps_pk PRIMARY KEY (id),
	CONSTRAINT dm_timestamps_un UNIQUE (ts),
	CONSTRAINT dm_timestamps_year_check CHECK (((year >= 2022) AND (year < 2500)))
);

-- Permissions

ALTER TABLE dds.dm_timestamps OWNER TO jovyan;
GRANT ALL ON TABLE dds.dm_timestamps TO jovyan;

-- dds.dm_users definition

-- Drop table

-- DROP TABLE dds.dm_users;

CREATE TABLE dds.dm_users (
	id serial4 NOT NULL,
	user_id varchar NOT NULL,
	user_name varchar NOT NULL,
	user_login varchar NOT NULL,
	CONSTRAINT dm_users_pk PRIMARY KEY (id),
	CONSTRAINT dm_users_un UNIQUE (user_id)
);

-- Permissions

ALTER TABLE dds.dm_users OWNER TO jovyan;
GRANT ALL ON TABLE dds.dm_users TO jovyan;

-- dds.dm_products definition

-- Drop table

-- DROP TABLE dds.dm_products;

CREATE TABLE dds.dm_products (
	id serial4 NOT NULL,
	product_id varchar NOT NULL,
	product_name varchar NOT NULL,
	product_price numeric(14, 2) NOT NULL DEFAULT 0,
	active_from timestamp NOT NULL DEFAULT now(),
	active_to timestamp NOT NULL DEFAULT now(),
	restaurant_id int4 NOT NULL,
	CONSTRAINT dm_products_check CHECK ((product_price >= (0)::numeric)),
	CONSTRAINT dm_products_pk PRIMARY KEY (id),
	CONSTRAINT dm_products_un UNIQUE (product_id, restaurant_id)
);

-- Permissions

ALTER TABLE dds.dm_products OWNER TO jovyan;
GRANT ALL ON TABLE dds.dm_products TO jovyan;


-- dds.dm_products foreign keys

ALTER TABLE dds.dm_products ADD CONSTRAINT dm_products_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id);

-- dds.dm_orders definition

-- Drop table

-- DROP TABLE dds.dm_orders;

CREATE TABLE dds.dm_orders (
	id serial4 NOT NULL,
	user_id int4 NOT NULL,
	restaurant_id int4 NOT NULL,
	timestamp_id int4 NOT NULL,
	order_key varchar NOT NULL,
	order_status varchar NOT NULL,
	CONSTRAINT dm_orders_pk PRIMARY KEY (id),
	CONSTRAINT dm_orders_un UNIQUE (order_key)
);

-- Permissions

ALTER TABLE dds.dm_orders OWNER TO jovyan;
GRANT ALL ON TABLE dds.dm_orders TO jovyan;


-- dds.dm_orders foreign keys

ALTER TABLE dds.dm_orders ADD CONSTRAINT dm_orders_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id);
ALTER TABLE dds.dm_orders ADD CONSTRAINT dm_orders_timestamps_id_fkey FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps(id);
ALTER TABLE dds.dm_orders ADD CONSTRAINT dm_orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES dds.dm_users(id);

-- dds.fct_product_sales definition

-- Drop table

-- DROP TABLE dds.fct_product_sales;

CREATE TABLE dds.fct_product_sales (
	id serial4 NOT NULL,
	product_id int4 NOT NULL,
	order_id int4 NOT NULL,
	count int4 NOT NULL DEFAULT 0,
	price numeric(14, 2) NOT NULL DEFAULT 0,
	total_sum numeric(14, 2) NOT NULL DEFAULT 0,
	bonus_payment numeric(14, 2) NOT NULL DEFAULT 0,
	bonus_grant numeric(14, 2) NOT NULL DEFAULT 0,
	CONSTRAINT fct_product_sales_bonus_grant_check CHECK ((bonus_grant >= (0)::numeric)),
	CONSTRAINT fct_product_sales_bonus_payment_check CHECK ((bonus_payment >= (0)::numeric)),
	CONSTRAINT fct_product_sales_count_check CHECK ((count >= 0)),
	CONSTRAINT fct_product_sales_id_pk PRIMARY KEY (id),
	CONSTRAINT fct_product_sales_price_check CHECK ((price >= (0)::numeric)),
	CONSTRAINT fct_product_sales_total_sum_check CHECK ((total_sum >= (0)::numeric)),
	CONSTRAINT fct_product_sales_un UNIQUE (product_id, order_id)
);

-- Permissions

ALTER TABLE dds.fct_product_sales OWNER TO jovyan;
GRANT ALL ON TABLE dds.fct_product_sales TO jovyan;


-- dds.fct_product_sales foreign keys

ALTER TABLE dds.fct_product_sales ADD CONSTRAINT fct_product_sales_order_id_fk FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id);
ALTER TABLE dds.fct_product_sales ADD CONSTRAINT fct_product_sales_product_id_fk FOREIGN KEY (product_id) REFERENCES dds.dm_products(id);

-- dds.srv_wf_settings definition

-- Drop table

-- DROP TABLE dds.srv_wf_settings;

CREATE TABLE dds.srv_wf_settings (
	id serial4 NOT NULL,
	workflow_key varchar NOT NULL,
	workflow_settings json NOT NULL,
	CONSTRAINT srv_wf_settings_pk PRIMARY KEY (workflow_key)
);

-- Permissions

ALTER TABLE dds.srv_wf_settings OWNER TO jovyan;
GRANT ALL ON TABLE dds.srv_wf_settings TO jovyan;

-- stg.bonussystem_events definition

-- Drop table

-- DROP TABLE stg.bonussystem_events;

CREATE TABLE stg.bonussystem_events (
	id int4 NOT NULL,
	event_ts timestamp NOT NULL,
	event_type varchar NOT NULL,
	event_value text NOT NULL,
	CONSTRAINT bonussystem_events_pk PRIMARY KEY (id)
);
CREATE INDEX bonussystem_events_event_ts_idx ON stg.bonussystem_events USING btree (event_ts);

-- Permissions

ALTER TABLE stg.bonussystem_events OWNER TO jovyan;
GRANT ALL ON TABLE stg.bonussystem_events TO jovyan;

-- stg.bonussystem_ranks definition

-- Drop table

-- DROP TABLE stg.bonussystem_ranks;

CREATE TABLE stg.bonussystem_ranks (
	id int4 NOT NULL,
	"name" varchar(2048) NOT NULL,
	bonus_percent numeric(19, 5) NOT NULL,
	min_payment_threshold numeric(19, 5) NOT NULL,
	CONSTRAINT bonussystem_ranks_pk PRIMARY KEY (id)
);

-- Permissions

ALTER TABLE stg.bonussystem_ranks OWNER TO jovyan;
GRANT ALL ON TABLE stg.bonussystem_ranks TO jovyan;

-- stg.bonussystem_users definition

-- Drop table

-- DROP TABLE stg.bonussystem_users;

CREATE TABLE stg.bonussystem_users (
	id int4 NOT NULL,
	order_user_id text NOT NULL,
	CONSTRAINT bonussystem_users_pk PRIMARY KEY (id)
);

-- Permissions

ALTER TABLE stg.bonussystem_users OWNER TO jovyan;
GRANT ALL ON TABLE stg.bonussystem_users TO jovyan;

-- stg.ordersystem_orders definition

-- Drop table

-- DROP TABLE stg.ordersystem_orders;

CREATE TABLE stg.ordersystem_orders (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT ordersystem_orders_object_id_uindex UNIQUE (object_id),
	CONSTRAINT ordersystem_orders_pk PRIMARY KEY (id)
);

-- Permissions

ALTER TABLE stg.ordersystem_orders OWNER TO jovyan;
GRANT ALL ON TABLE stg.ordersystem_orders TO jovyan;

-- stg.ordersystem_restaurants definition

-- Drop table

-- DROP TABLE stg.ordersystem_restaurants;

CREATE TABLE stg.ordersystem_restaurants (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT ordersystem_restaurants_object_id_uindex UNIQUE (object_id),
	CONSTRAINT ordersystem_restaurants_pk PRIMARY KEY (id)
);

-- Permissions

ALTER TABLE stg.ordersystem_restaurants OWNER TO jovyan;
GRANT ALL ON TABLE stg.ordersystem_restaurants TO jovyan;

-- stg.ordersystem_users definition

-- Drop table

-- DROP TABLE stg.ordersystem_users;

CREATE TABLE stg.ordersystem_users (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT ordersystem_users_object_id_uindex UNIQUE (object_id),
	CONSTRAINT ordersystem_users_pk PRIMARY KEY (id)
);

-- Permissions

ALTER TABLE stg.ordersystem_users OWNER TO jovyan;
GRANT ALL ON TABLE stg.ordersystem_users TO jovyan;

-- stg.srv_wf_settings definition

-- Drop table

-- DROP TABLE stg.srv_wf_settings;

CREATE TABLE stg.srv_wf_settings (
	id serial4 NOT NULL,
	workflow_key varchar NOT NULL,
	workflow_settings json NOT NULL,
	CONSTRAINT srv_wf_settings_pk PRIMARY KEY (workflow_key)
);

-- Permissions

ALTER TABLE stg.srv_wf_settings OWNER TO jovyan;
GRANT ALL ON TABLE stg.srv_wf_settings TO jovyan;