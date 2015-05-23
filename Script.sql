--# Postgresql

UPDATE practise as p
SET costs = cte.ci *60
FROM 
(
  SELECT id,imp/sum(imp) over(partition by adid) as ci
	FROM practise
) as cte
WHERE p.id=cte.id
