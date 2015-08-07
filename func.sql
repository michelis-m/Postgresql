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
