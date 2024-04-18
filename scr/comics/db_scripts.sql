CREATE TABLE etl.load_status (
	load_status_key serial,
	table_name varchar(50) NULL,
	count_records int4 NULL,
	min_value int8 null,
	max_value int8 null,
	executed_at timestamp NULL
);
CREATE UNIQUE INDEX load_status_load_status_key ON etl.load_status USING btree (load_status_key);


CREATE TABLE public.xkcd_records (
	num int8 NOT NULL,
	link text NULL,
	news text NULL,
	safe_title text NULL,
	transcript text NULL,
	alt text NULL,
	img text NULL,
	title text NULL,
	cost_eur int8 NULL,
	created_at timestamp NULL,
	CONSTRAINT xkcd_records_pkey PRIMARY KEY (num)
);
CREATE UNIQUE INDEX xkcd_records_num ON public.xkcd_records USING btree (num);


create table public.reviews (
	review_id serial,
	num int8 not null ,
	rating float null,
	review_date date null,
	snapshot_date date null,
	primary key (review_id),
	constraint fk_reviews
      foreign key (num)
        references xkcd_records(num)
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
	CONSTRAINT fk_daily_views FOREIGN KEY (num) REFERENCES public.xkcd_records(num)
);
CREATE UNIQUE INDEX views_row_key ON public.daily_views USING btree (row_key);
CREATE INDEX daily_views_num_date ON public.daily_views USING btree (num, view_date);



CREATE OR REPLACE VIEW public.v_xkcd_data
AS
WITH reviews AS (
    SELECT r_1.num, count(1) AS reviews_count, avg(r_1.rating) AS average_rating
    FROM public.reviews r_1
    GROUP BY r_1.num
), total_views AS (
    SELECT dv.num, sum(dv.views_count) AS total_views
    FROM daily_views dv
    GROUP BY dv.num
)
SELECT
    xr.*,
    r.reviews_count,
    r.average_rating,
    tv.total_views
FROM xkcd_records xr
LEFT JOIN reviews r ON r.num = xr.num
LEFT JOIN total_views tv ON tv.num = xr.num;