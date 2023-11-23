CREATE TABLE stg.deliverysystem_deliveries (
	id serial4 NOT NULL,
	request_id varchar NOT NULL,
	request_date timestamp NOT NULL,
	response text NOT NULL,
	CONSTRAINT deliverysystem_deliveries_pk PRIMARY KEY (request_id),
	CONSTRAINT deliverysystem_deliveries_un UNIQUE (response)
);

CREATE TABLE stg.deliverysystem_couriers (
	id serial4 NOT NULL,
	request_id varchar NOT NULL,
	request_date timestamp NOT NULL,
	response text NOT NULL,
	CONSTRAINT deliverysystem_couriers_pk PRIMARY KEY (request_id),
	CONSTRAINT deliverysystem_couriers_un UNIQUE (response)
);