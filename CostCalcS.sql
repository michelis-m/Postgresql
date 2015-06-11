SELECT './junk/msgPostStg 20120 '||job.batch_id||' ;'||' ./junk/msgPostStg 20122 '||job.batch_id||' ;'||' ./junk/msgPostStg 20123 '||job.batch_id||' ;'||' ./junk/msgPostStg 20124 '||job.batch_id||' ;'||' ./junk/msgPostStg 20125 '||job.batch_id||' ;'||' ./junk/msgPostStg 20126 '||job.batch_id||' ;'||' ./junk/msgPostStg 20127 '||job.batch_id||' ;'
FROM job
INNER JOIN task ON job.task_id = task.task_id
INNER JOIN client ON task.client_id = client.client_id
INNER JOIN client AS client_1 ON job.client_id = client_1.client_id
INNER JOIN status ON job.status_id = status.status_id
WHERE task.task_id = 20105
AND status.statusval = 'MATRIXLOADER_COMPLETE'
AND job.task_date BETWEEN '2014-12-01' AND '2015-02-28'
