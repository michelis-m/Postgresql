create or replace function log_batch(b_id int,mo_id int)
returns text
LANGUAGE plpgsql
as '
DECLARE beid int;

begin
--increases the batch_execution id by 1
beid := (select batch_execution_id from dan_control.batch_execution order by batch_execution_id desc limit 1) + 1;

--checks if batch_id already exists, then checks status. If doesnt exist is inserted with status 0, if exists status changes to 1
IF exists(select 1 from dan_control.batch_execution where batch_id=b_id) IS TRUE THEN
	IF (select status_id from dan_control.batch_execution where batch_id=b_id) = 0 THEN
		update dan_control.batch_execution set status_id = 1, batch_updated = now() where batch_id=b_id;
	return 0; -- updated and completed
	ELSE
	return 99 ; --already processed
	END IF;
--return 0;
ELSE
insert into dan_control.batch_execution
(batch_created, batch_id, metaobject_id, batch_aggregated, batch_execution_id, status_id, batch_updated)
values
(now(), b_id, mo_id, false, beid, 0, now());

return 1 ; --inserted and processing
END IF;

end
';

---------------------------------------------------------------------------------------------------------------

create function startx6(t_id int)
returns table(a int, b int)
as
$body$ 
begin
--while end loop;
IF EXISTS( --The following query checks for given task_id, if the batches for task days are on status 14
with cte as (
        select count(1) ct,tl.task_id from dan_control.task_lookback tl --selecting the count of tasks in status 1 and relevant date
        inner join dan_control.job j on j.task_id=tl.task_id 
        where j.task_id = t_id 
        and status_id = 14
        and task_date >= (select date_trunc('day',now()) -(days * interval '1 day')
                                        from dan_control.task_lookback
                                                where task_id = t_id ) 
        group by tl.task_id
)select days from dan_control.task_lookback tl  --comparing the number of count with the number of days for this task
inner join cte on cte.task_id = tl.task_id
where tl.task_id = t_id and tl.days = cte.ct )  IS TRUE 
THEN    
                return query select 201760000 task_id, b_id batch_id;
--execute task 20176000 if criteria are completed
end if; 
end;    
$body$                                  
LANGUAGE plpgsql; 


