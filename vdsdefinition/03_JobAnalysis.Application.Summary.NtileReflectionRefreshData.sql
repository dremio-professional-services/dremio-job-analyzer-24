create or replace VDS JobAnalysis.Application.Summary.NtilReflectionRefreshData
as
with T1 as (SELECT "pct", MAX("queryCost") AS "maxQueryCost"
FROM (SELECT "queryCost", (NTILE(100) OVER (ORDER BY "queryCost")) AS "pct"
FROM "JobAnalysis"."Business"."ReflectionRefreshData" where outcome='COMPLETED') AS "a"
GROUP BY "pct"
ORDER BY "pct")
,
T3 as (SELECT "pct", MAX("totalDurationMS") AS "totalDurationMS"
FROM (SELECT "totalDurationMS", (NTILE(100) OVER (ORDER BY "totalDurationMS")) AS "pct"
FROM "JobAnalysis"."Business"."ReflectionRefreshData"  where outcome='COMPLETED') AS "a"
GROUP BY "pct"
ORDER BY "pct")
,
T4 as (SELECT "pct", MAX("outputRecords") AS "outputRecords"
FROM (SELECT "outputRecords", (NTILE(100) OVER (ORDER BY "outputRecords")) AS "pct"
FROM "JobAnalysis"."Business"."ReflectionRefreshData"  where outcome='COMPLETED') AS "a"
GROUP BY "pct"
ORDER BY "pct")

select T1.pct,T1.maxQueryCost, T3.totalDurationMS,T4.outputRecords from T1,T3,T4
where
T1.pct=T3.pct and T3.pct=T4.pct
order by T1.pct
