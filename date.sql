select to_date(to_char((extract( year from now())) * 10000 + (extract( month from now())) * 100 + (select days from dan_control.task_lookback where task_id = 20105),'00000000'), 'yyyymmdd')


select to_date(to_char((extract( year from now())) * 10000 + (extract( month from now())) * 100 + (extract( day from now()))- (select days from dan_control.task_lookback where task_id = 20105) ,'00000000'), 'yyyymmdd')


select now()+((days*-1) * interval '1 day'),days,now()
from dan_control.task_lookback, dan_control.job  
where task_id = 20105
