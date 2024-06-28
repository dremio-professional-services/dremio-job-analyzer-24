CREATE OR REPLACE VDS 
JobAnalysis.Business.MDRefreshData
AS
SELECT *
  FROM (SELECT "queryId", "queryText",  SPLIT_PART(queryText,' ',3) as dsName,"queryChunkSizeBytes", "nrQueryChunks", "startTime", "finishTime", CAST("totalDurationMS" / 60000.000 AS DECIMAL(10, 3)) AS "totalDurationMinutes", CAST("totalDurationMS" / 1000.000 AS DECIMAL(10, 3)) AS "totalDurationSeconds", "totalDurationMS", ROW_NUMBER() OVER (ORDER BY "totalDurationMS" DESC) AS "rownumByTotalDurationMS", "outcome", "username", "requestType", "queryType", "parentsList", "queueName", "poolWaitTime", "planningTime", "enqueuedTime", "executionTime","context", "engineName", "accelerated", "inputRecords", "inputBytes", "outputRecords", "outputBytes", "queryCost"
  ,"memoryAllocated","setupTimeNs","waitTimeNs","executionNodes",size(executionNodes) "executionNodeCnt","executionCpuTimeNS"
  "outcomereason", "CONCAT"('http://<DREMIO_HOST>:9047/jobs?#', "queryId") AS "profileUrl"
  FROM "JobAnalysis"."Preparation"."results" AS "results"
  WHERE queryType='METADATA_REFRESH') As "t"
WHERE ("rownumByTotalDurationMS" > 0)
