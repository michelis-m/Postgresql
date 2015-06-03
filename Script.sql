--# Postgresql

UPDATE practise as p
SET costs = cte.ci *60
FROM 
(
  SELECT id,imp/sum(imp) over(partition by adid) as ci
	FROM practise
) as cte
WHERE p.id=cte.id

------------------- Counts ----------------------------------------------------

WITH C AS
(
	SELECT dan_sequence_id , ROW_NUMBER() OVER (PARTITION BY dan_sequence_id ORDER BY (SELECT NULL)) AS n
	FROM public.adjusted_rich_media_metrics1_728656
)
SELECT * FROM C WHERE n>1

------------------- Counts 2 ----------------------------------------------------

select count(1),count(distinct dan_sequence_Id) from [schema][dot][table] having count(1)<>count(distinct dan_sequence_Id) ;



select 'select count(1),count(distinct dan_sequence_Id) from '||table_schema||'.'||table_name||' having count(1)<>count(distinct dan_sequence_Id)'||' union'
from information_schema.tables
where table_type = 'BASE TABLE'
and table_schema = 'public'
and table_name like 'adjusted_%';
