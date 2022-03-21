with 
aux1
as (
select user_id from (
select user_id
,count(event_description) as login_count
,ROW_NUMBER() OVER (ORDER BY count(event_description) DESC) AS rk
from dim_user_day_activity where event_description='login' and date_part('month',fecha) = 1 group by user_id -- supongo registros mes de enero
) tmp where rk < 4)
-- select * from aux1;
,aux2
as (
SELECT user_id, min(fecha) AS StartDay, max(fecha) AS EndDay, max(fecha)-min(fecha) as daydiff from (
SELECT user_id, fecha, rn1, rn2, rn1 - rn2 AS GroupNumber from	
(select *
	,extract(doy from fecha) as rn1 -- doy: day of year
	,ROW_NUMBER() OVER (PARTITION by user_id ORDER BY fecha ASC) AS rn2
	from (
 select *
    ,ROW_NUMBER() OVER (PARTITION by user_id,fecha ORDER by fecha) AS rn0
	FROM (
		select *
		,CASE WHEN max_time_spent>300 and event_description != 'login' THEN 1 ELSE 0 end as eventOK
		from dim_user_day_activity where user_id in (select * from aux1)
	) a where a.eventOK = 1) b where rn0 = 1) tmp1 ) tmp2 GROUP BY user_id, GroupNumber
)
SELECT ((SELECT 1.0*COUNT(distinct user_id) FROM aux2 where daydiff > 0) / (SELECT COUNT(distinct user_id) FROM dim_user_day_activity)) * 100 as percentage;

-- select count(distinct user_id) from aux where daydiff > 0;
-- select count(distinct user_id) from dim_user_day_activity;
