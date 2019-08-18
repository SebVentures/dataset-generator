create table customer (
	id bigint not null, 
	name varchar(256) not null, 
	acquisition_channel varchar(256) not null,
	persona varchar(256) not null
);

create table lifetime (
	customer_id bigint not null, 
	date date not null, 
	mrr double precision not null
);