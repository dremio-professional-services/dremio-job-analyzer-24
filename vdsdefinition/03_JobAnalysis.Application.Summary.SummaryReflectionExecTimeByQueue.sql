create or replace VDS JobAnalysis.Application.Summary.SummaryReflectionExecTimeByQueue
as
WITH "T1" AS (SELECT  cast(startTime as date) sdate, "queueName",queryCost, 1 AS "QueryCount",
(CASE WHEN (((COALESCE("enqueuedTime")) / 1000) <= 0) THEN 0 ELSE 1 END) AS "WaitQuery",
((COALESCE("enqueuedTime", 0)) / 1000) AS "QueueTimeSec",
((COALESCE("executionTime", 0)) / 1000) AS "ExecTimeSec",
poolWaitTime/1000 as PoolTimeSec, planningTime/1000 as PlanTimeSec
FROM JobAnalysis.Business.ReflectionRefreshData
WHERE (("queueName" <> '') AND ("outcome" = 'COMPLETED')))
(SELECT "queueName", max(queryCost) MaxQueryCost, SUM("QueryCount") AS "TotalQueryCount",
 SUM("WaitQuery") AS "QueuedReflections",
 ((100 * SUM("WaitQuery")) / SUM("QueryCount")) AS "PercentageQueued",
MAX("PlanTimeSec") AS "MaxPlanTimeSec", AVG("PlanTimeSec") AS "AvgPlanTimeSec",
MAX("QueueTimeSec") AS "MaxQueueTimeSec", AVG("QueueTimeSec") AS "AvgQueueTimeSec",
MAX("ExecTimeSec") AS "MaxExecTimeSec", (AVG("ExecTimeSec")) AS "AvgExecTimeSec",
PERCENTILE_DISC(0.8) WITHIN GROUP ( ORDER BY ExecTimeSec ASC ) "80thPercentile"

FROM "T1"
GROUP BY "queueName")
ORDER BY 1 DESC
