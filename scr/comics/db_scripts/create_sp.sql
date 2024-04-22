CREATE OR REPLACE PROCEDURE public.sp_xkcd_webcomics()
 LANGUAGE plpgsql
AS $procedure$
declare
processing_time timestamp := now();
start_time timestamp := clock_timestamp( );
insert_count_reviews int := 0;
insert_count_views int := 0;


begin

drop table if exists xkcd_reviews_temp;
drop table if exists xkcd_daily_views_temp;


create temp table xkcd_reviews_temp
as
SELECT r_1.num, count(1) AS review_count, avg(r_1.rating) AS review_average
FROM public.reviews r_1
GROUP BY r_1.num ;


create temp table xkcd_daily_views_temp
as
SELECT dv.num, sum(dv.views_count) AS total_views
FROM daily_views dv
GROUP BY dv.num;


update public.xkcd_webcomics
set
	review_count = reviews_temp.review_count,
	review_average = reviews_temp.review_average
from
	xkcd_reviews_temp as reviews_temp
where
	xkcd_webcomics.num = reviews_temp.num
	and (coalesce(xkcd_webcomics.review_count,0) <> reviews_temp.review_count
		or coalesce(xkcd_webcomics.review_average,0) <> reviews_temp.review_average);


update public.xkcd_webcomics
set
	total_views = views_temp.total_views
from
	xkcd_daily_views_temp as views_temp
where
	xkcd_webcomics.num = views_temp.num
	and coalesce(xkcd_webcomics.total_views,0) <> views_temp.total_views;


-------Calculate count of added records

	select count(a.num) 
	from xkcd_reviews_temp a
	left join public.xkcd_webcomics b on a.num=b.num and (a.review_count<>b.review_count or a.review_average<>b.review_average)
	where b.review_count is null or b.review_average is null;

	select count(a.num) into insert_count_views 
	from xkcd_daily_views_temp a
	left join public.xkcd_webcomics b on a.num=b.num and a.total_views<>b.total_views
	where b.total_views is null;


-------Insert entry to ETL procedures table

INSERT INTO etl.load_status
      (procedure_name,
      table_name,
       count_records,
       executed_at)
    VALUES
      ('public.sp_xkcd_webcomics() - reviews',
       'public.xkcd_webcomics',
       insert_count_reviews,
       processing_time),
      ('public.sp_xkcd_webcomics() - views',
       'public.xkcd_webcomics',
       insert_count_views,
       processing_time);



end;

$procedure$
;


