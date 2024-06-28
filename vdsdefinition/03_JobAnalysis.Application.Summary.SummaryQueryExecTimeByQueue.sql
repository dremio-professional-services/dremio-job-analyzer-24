create or replace VDS JobAnalysis.Application.Summary.SummaryQueryExecTimeByQueue
as
WITH "T1" AS (SELECT cast(startTime as date) sdate,requestType, 1 AS "QueryCount", queueName,
    (CASE WHEN (((COALESCE("enqueuedTime")) / 1000) <= 0) THEN 0 ELSE 1 END) AS "WaitQuery",
    (COALESCE("poolWaitTime", 0)/1000)  AS "PoolTimeSec",
    (COALESCE("planningTime", 0)/1000) AS "PlanTimeSec",
    (COALESCE("enqueuedTime", 0)/1000) AS "QueueTimeSec",
    (COALESCE("executionTime", 0)/1000) AS "ExecTimeSec",queryCost --,memoryAllocated
    FROM "JobAnalysis"."Business"."SelectQueryData" WHERE ((outcome in ('COMPLETED')) ) )
(SELECT  "queueName", SUM("QueryCount") AS "TotalQueryCount",
MAX("queryCost") AS "MaxQueryCost",
-- MAX("memoryAllocated")/1048576 AS "MaxMemoryMB",
((100 * SUM("WaitQuery")) / SUM("QueryCount")) AS "Queued %",
cast (AVG("PoolTimeSec") as decimal(5,2)) AS "AvgPoolTimeSec",
cast (AVG("PlanTimeSec") as decimal(5,2)) AS "AvgPlanTimeSec",
MAX("QueueTimeSec") AS "MaxQueueTimeSec",
cast(AVG("QueueTimeSec") as decimal(5,2)) AS "AvgQueueTimeSec",
MAX("ExecTimeSec") AS "MaxExecTimeSec",
cast (AVG("ExecTimeSec") as decimal(5,2)) AS "AvgExecTimeSec",
PERCENTILE_DISC(0.8) WITHIN GROUP ( ORDER BY ExecTImeSec ASC ) "80thPercentileExecution"
FROM "T1"
GROUP BY 1)
ORDER BY 1 DESC

