CREATE OR REPLACE VDS 
JobAnalysis.Business.ReflectionRefreshData
AS
SELECT *
  FROM (SELECT "queryId", "queryText", CASE WHEN length(substr(queryText, 21, 36)) > 0 THEN substr(queryText, 21, 36) ELSE NULL END AS reflection_id,"queryChunkSizeBytes", "nrQueryChunks", "startTime", "finishTime", CAST("totalDurationMS" / 60000.000 AS DECIMAL(10, 3)) AS "totalDurationMinutes", CAST("totalDurationMS" / 1000.000 AS DECIMAL(10, 3)) AS "totalDurationSeconds", "totalDurationMS", ROW_NUMBER() OVER (ORDER BY "totalDurationMS" DESC) AS "rownumByTotalDurationMS", "outcome", "username", "requestType", "queryType", "parentsList", "queueName", "poolWaitTime", "planningTime", "enqueuedTime", "executionTime","context", "engineName",  "accelerated", "inputRecords", "inputBytes", "outputRecords", "outputBytes", "queryCost"
  ,"memoryAllocated","setupTimeNs","waitTimeNs","executionNodes",size(executionNodes) "executionNodeCnt","executionCpuTimeNS"
  "outcomereason", "CONCAT"('http://<DREMIO_HOST>:9047/jobs?#', "queryId") AS "profileUrl"
  FROM "JobAnalysis"."Preparation"."results" AS "results"
  WHERE queryType='ACCELERATOR_CREATE' and "SUBSTR"(UPPER("queryText"), "STRPOS"(UPPER("queryText"), 'REFRESH REFLECTION')) LIKE 'REFRESH REFLECTION%')  AS "t"

WHERE ("rownumByTotalDurationMS" > 0)
