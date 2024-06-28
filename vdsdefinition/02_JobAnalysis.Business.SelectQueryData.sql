CREATE OR REPLACE VDS 
JobAnalysis.Business.SelectQueryData 
AS
SELECT *
FROM (SELECT "queryId", "queryText", "queryChunkSizeBytes", "nrQueryChunks", "startTime", "finishTime",
CAST("totalDurationMS" / 60000.000 AS DECIMAL(10, 3)) AS "totalDurationMinutes",
CAST("totalDurationMS" / 1000.000 AS DECIMAL(10, 3)) AS "totalDurationSeconds",
"totalDurationMS", ROW_NUMBER() OVER (ORDER BY "totalDurationMS" DESC) AS "rownumByTotalDurationMS",
"outcome", "username", "requestType", "queryType", "parentsList", "queueName", 
poolWaitTime,
planningTime,
enqueuedTime,
executionTime,
 "context","engineName",
"accelerated", "inputRecords", "inputBytes", "outputRecords", "memoryAllocated","outputBytes",
"queryCost", "outcomereason", "CONCAT"('http://<DREMIO_HOST>:9047/jobs?#', "queryId") AS "profileUrl","setupTimeNs","waitTimeNs",executionNodes
,size(executionNodes) "executionNodeCnt","executionCpuTimeNS"
FROM "JobAnalysis"."Preparation"."results" AS "results"
WHERE "SUBSTR"(UPPER("queryText"), "STRPOS"(UPPER("queryText"), 'SELECT')) LIKE 'SELECT%' AND UPPER("queryText") NOT LIKE 'CREATE TABLE%'
) AS "t"
WHERE "rownumByTotalDurationMS" > 0
