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

select count(1) AS RowsCount,count(distinct site_id) AS UniqueRows,(count(1) - count(distinct site_id)) AS Duplicates 
from pdw.site having count(1)<>count(distinct site_id) ; 

-------------------------------------------------------------------------------------------

select 'select count(1),count(distinct dan_sequence_Id) from '||table_schema||'.'||table_name||' having count(1)<>count(distinct dan_sequence_Id)'||' union'
from information_schema.tables
where table_type = 'BASE TABLE'
and table_schema = 'public'
and table_name like 'adjusted_%';

-------------------------------------------------------------------------------------------
INSERT INTO canonical_dfa.advertiser_swap
(advertiser, advertiser_id, dan_row_start_date)
WITH C AS (
	select *
	from canonical_dfa.advertiser_temp a
	where dan_batch_id = 730676 
	AND (
		NOT EXISTS 
		(select *
		from canonical_dfa.stg_advertiser_temp b 
		where a.advertiser_id=b.advertiser_id
		)
		OR NOT EXISTS(select *
		from canonical_dfa.stg_advertiser_temp b 
		where a.advertiser=b.advertiser
		) 
	)
),
CTE AS (
	SELECT advertiser,advertiser_id, Max(dan_row_start_date) as maxd
	FROM (SELECT * FROM C UNION ALL SELECT * FROM canonical_dfa.advertiser_temp) as d
	group by advertiser,advertiser_id
)
SELECT advertiser,advertiser_id, maxd from CTE

-----------   CBA    ----------------------------------------------------------------------

WITH CTE AS
(
	SELECT * FROM canonical_dfa.stg_advertiser_temp
	UNION
	SELECT * FROM canonical_dfa.advertiser_temp
), CTE2 AS
(
SELECT RANK() OVER (PARTITION BY advertiser_id ORDER BY dan_row_start_date desc) AS rnk,* FROM CTE
)
SELECT * FROM CTE2 WHERE rnk=1


-----------regexp-----------------------------
select sql_clob,regexp_matches(sql_clob,'\"(\d{6})\"\,\"(\d{6})\"\,\"(\d{6})\"\,\"(\d{6})\"\,\"(\d{6})\"', 'g') 
from dan_control.sql_task where task_id = 201760000 and task_sequence = 1
