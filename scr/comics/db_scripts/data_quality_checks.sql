-- REFERENTIAL INTEGRITY

SELECT *
FROM public.reviews r
LEFT JOIN public.xkcd_webcomics x ON r.num = x.num
WHERE x.num IS NULL;

SELECT *
FROM public.daily_views dv
LEFT JOIN public.xkcd_webcomics x ON dv.num = x.num
WHERE x.num IS NULL;

-- DUPLICATE VALUES

SELECT num, COUNT(*)
FROM public.xkcd_webcomics
GROUP BY num
HAVING COUNT(*) > 1;

SELECT review_id, COUNT(*)
FROM public.reviews
GROUP BY review_id
HAVING COUNT(*) > 1;

SELECT num, view_date, COUNT(*)
FROM public.daily_views
GROUP BY num, view_date
HAVING COUNT(*) > 1;

-- COMPLETENESS, are there any columns missing 90% of values or more

with blanks as (
	select
	(num / all_records) as num,
	(safe_title / all_records) as safe_title,
	(transcript / all_records) as transcript,
	(alt / all_records) as alt,
	(img / all_records) as img,
	(title / all_records) as title
	from (
	SELECT
		count(1)::float as all_records,
	    SUM(CASE WHEN num IS NULL THEN 1 ELSE 0 END)::float AS num,
	    SUM(CASE WHEN safe_title IS NULL THEN 1 ELSE 0 END) AS safe_title,
	    SUM(CASE WHEN transcript IS NULL THEN 1 ELSE 0 END) AS transcript,
	    SUM(CASE WHEN alt IS NULL THEN 1 ELSE 0 END) AS alt,
	    SUM(CASE WHEN img IS NULL THEN 1 ELSE 0 END) AS img,
	    SUM(CASE WHEN title IS NULL THEN 1 ELSE 0 END) AS title
	from public.xkcd_webcomics x )
)
select  *
from blanks
where num > 0.9 or safe_title > 0.9 or transcript > 0.9 or alt > 0.9 or img > 0.9 or title > 0.9
;


-- VALIDITY

SELECT * FROM public.reviews WHERE rating < 1 OR rating > 10;
SELECT * FROM public.xkcd_webcomics WHERE review_average < 1 OR review_average > 10;
SELECT * FROM public.xkcd_webcomics WHERE cost_eur < 0;
SELECT * FROM public.daily_views WHERE view_date IS NULL;

