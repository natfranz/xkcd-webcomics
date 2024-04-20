CREATE OR REPLACE VIEW public.v_xkcd_webcomics
AS SELECT num,
    safe_title,
    transcript,
    alt,
    img,
    title,
    cost_eur,
    created_at,
    review_count,
    review_average,
    total_views
   FROM xkcd_webcomics;