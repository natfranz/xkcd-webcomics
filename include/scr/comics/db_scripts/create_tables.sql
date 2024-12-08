CREATE TABLE etl.load_status (
	load_status_key serial,
	procedure_name varchar(50) NULL,
	table_name varchar(50) NULL,
	count_records int4 NULL,
	min_value int8 NULL,
	max_value int8 NULL,
	executed_at timestamp NULL
);
CREATE UNIQUE INDEX load_status_load_status_key ON etl.load_status USING btree (load_status_key);


CREATE TABLE public.xkcd_webcomics (
	num int8 NOT NULL,
	safe_title text NULL,
	transcript text NULL,
	alt text NULL,
	img text NULL,
	title text NULL,
	cost_eur int8 NULL,
	created_at date NULL,
	review_count int8 NULL,
	review_average float8 NULL,
	total_views int8 NULL,
	CONSTRAINT xkcd_records_pkey PRIMARY KEY (num)
);
CREATE UNIQUE INDEX xkcd_records_num ON public.xkcd_webcomics USING btree (num);


create table public.reviews (
	review_id serial,
	num int8 not null ,
	rating float null,
	review_date date null,
	snapshot_date date null,
	primary key (review_id),
	constraint fk_reviews
      foreign key (num)
        references public.xkcd_webcomics(num)
);
CREATE UNIQUE INDEX reviews_review_id ON public.reviews USING btree (review_id);
CREATE INDEX reviews_num_date ON public.reviews USING btree (num, review_date);


CREATE TABLE public.daily_views (
	row_key serial4 NOT NULL,
	view_date date NULL,
	num int8 NOT NULL,
	views_count int8 null,
	snapshot_date date null,
	CONSTRAINT daily_views_pkey PRIMARY KEY (row_key),
	CONSTRAINT fk_daily_views FOREIGN KEY (num) REFERENCES public.xkcd_webcomics(num)
);
CREATE UNIQUE INDEX views_row_key ON public.daily_views USING btree (row_key);
CREATE INDEX daily_views_num_date ON public.daily_views USING btree (num, view_date);