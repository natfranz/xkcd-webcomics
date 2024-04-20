CREATE OR REPLACE FUNCTION public.func_xkcd_webcomics()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
BEGIN
        call public.sp_xkcd_webcomics();

END
$function$
;