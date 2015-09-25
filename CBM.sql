

-------------------------- To be added to relevant sequences -------------------------------------------
#if ($metaobject_category=="non-incremental")
#end
---------------------------- Creating Table Swap and inserting unique values ----------------

#set ( $version = "20150608" )

#set ( $dot = ".")
#set ( $underscore = "_")

#set ( $table = "advertiser" )
#set ( $schema = "canonical_dfa" )
#set ($listKeys = ["mdm_client_id","mdm_data_source_id","advertiser_id","mdm_account_id"])
#set ($listAttributes = ["advertiser", "advertiser_group", "mdm_advertiser_id", "dan_label_id", "advertiser_group_id", "spot_id"])
#set ($dan = ["dan_row_start_date","dan_row_end_date", "dan_batch_id", "dan_previous_batch_id", "dan_row_active"])


CREATE TABLE $schema.$table${underscore}swap$version (LIKE $schema.$table);

INSERT INTO $schema.$table${underscore}swap$version (
#foreach ($attribute in $listAttributes) $attribute, #end
#foreach ($dan in $dan) $dan, #end
#foreach($key in $listKeys) 
#if ($foreach.last) $key #else $key, #end 
#end
)
WITH CTE AS
(
	SELECT #foreach ($attribute in $listAttributes) $attribute, #end
			#foreach ($dan in $dan) $dan, #end
			#foreach($key in $listKeys) #if ($foreach.last) $key #else $key, #end #end FROM $schema.$table
	UNION
	SELECT #foreach ($attribute in $listAttributes) $attribute, #end
			#foreach ($dan in $dan) $dan, #end
			#foreach($key in $listKeys) #if ($foreach.last) $key #else $key, #end #end FROM $schema${dot}stg${underscore}$table
), CTE2 AS
(
  SELECT RANK() OVER (PARTITION BY #foreach($key in $listKeys) #if ($foreach.last) $key #else $key, #end#end ORDER BY dan_row_start_date DESC) AS rnk , 
			#foreach ($attribute in $listAttributes) $attribute, #end
			#foreach ($dan in $dan) $dan, #end
			#foreach($key in $listKeys) #if ($foreach.last) $key #else $key, #end #end
  FROM CTE
  WHERE dan_batch_id = $batch_Id
  )
SELECT 
#foreach ($attribute in $listAttributes) $attribute, #end
#foreach ($dan in $dan) $dan, #end
#foreach($key in $listKeys) #if ($foreach.last) $key #else $key, #end #end 
FROM CTE2 WHERE rnk=1;

----------------------- Renaming/Granting Permissions -------------------------------------------

alter table $schema.$table rename to $table${underscore}old$version;
alter table $schema.$table${underscore}swap$version rename to $table;

GRANT ALL ON $schema.$table TO GROUP dadl_app; 
GRANT SELECT ON $schema.$table TO GROUP dadl_read; 
GRANT SELECT ON $schema.$table TO GROUP read_pdw_gmteam; 
GRANT SELECT ON $schema.$table TO GROUP write_pdw_gmteam; 

-------------------------- Creating duplicates table ------------------------------------

CREATE TABLE $schema.$table${underscore}duplicates$version (LIKE $schema.$table);

INSERT INTO $schema.$table${underscore}duplicates$version (
#foreach ($attribute in $listAttributes) $attribute, #end
#foreach ($dan in $dan) $dan, #end
#foreach($key in $listKeys) 
#if ($foreach.last) $key #else $key, #end 
#end
)
WITH C AS
(
	SELECT #foreach ($attribute in $listAttributes) $attribute, #end
			#foreach ($dan in $dan) $dan, #end
			#foreach($key in $listKeys) $key, #end ROW_NUMBER() OVER (PARTITION BY #foreach($key in $listKeys) #if ($foreach.last) $key #else $key, #end#end ORDER BY (SELECT NULL)) AS n
	FROM $schema.$table where dan_row_active=1
)
SELECT #foreach ($attribute in $listAttributes) $attribute, #end
#foreach ($dan in $dan) $dan, #end
#foreach($key in $listKeys) #if ($foreach.last) $key #else $key, #end #end FROM C WHERE n>1;

------------------------ Creating View ------------------------------------------


#set ( $schema = "canonical_dfa")

#set ( $version = $batchId )
#set ( $dot = ".")
#set ( $underscore = "_")
#set ($dan = ["dan_row_start_date","dan_row_end_date", "dan_batch_id", "dan_previous_batch_id", "dan_row_active"])


#set( $labelgroup = "any_tx" )
#set( $label = "$labelgroup${underscore}999205.001" )
SET QUERY_GROUP='$label${underscore}$batchId';


CREATE OR REPLACE VIEW $schema.$table${underscore}cbmview AS
WITH CTE AS
(
  SELECT RANK() OVER (PARTITION BY #foreach($key in $listKeys) #if ($foreach.last) $key #else $key, #end#end ORDER BY dan_row_start_date DESC) AS dan_row_active , 
	isnull(LAG(dan_batch_id)OVER (PARTITION BY #foreach($key in $listKeys) #if ($foreach.last) $key #else $key, #end#end ORDER BY dan_row_start_date) ,dan_batch_id) AS dan_previous_batch_id,
	isnull(DATEADD('days',-1,LEAD(dan_row_start_date)OVER (PARTITION BY #foreach($key in $listKeys) #if ($foreach.last) $key #else $key, #end#end ORDER BY dan_row_start_date)),'9999-01-01')dan_row_end_date,
			#foreach ($attribute in $listAttributes) $attribute, #end
			#foreach ($dan in $dan) #if($dan != "dan_previous_batch_id"&& $dan != "dan_row_end_date"&& $dan != "dan_row_active" )  $dan, #end #end
			#foreach($key in $listKeys) #if ($foreach.last) $key #else $key, #end #end
  FROM $schema${dot}$table
  ) 
 SELECT 
#foreach ($attribute in $listAttributes) $attribute, #end
#foreach ($dan in $dan)  $dan, #end
#foreach($key in $listKeys) #if ($foreach.last) $key #else $key, #end #end 
FROM CTE WHERE dan_row_active=1;



