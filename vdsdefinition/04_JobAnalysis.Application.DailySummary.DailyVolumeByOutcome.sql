CREATE OR REPLACE VDS
JobAnalysis.Application.DailySummary.DailyVolumeByOutcome
AS
select *,COMPLETED+FAILED+CANCELED as total from (
    SELECT cast(startTime as date) startdate,outcome FROM JobAnalysis.Business.SelectQueryData
    UNION ALL
    SELECT cast(startTime as date) startdate,outcome FROM JobAnalysis.Business.MDRefreshData
    UNION ALL
    SELECT cast(startTime as date) startdate,outcome FROM JobAnalysis.Business.ReflectionRefreshData
)
PIVOT (
    count(outcome) for "outcome" in ('COMPLETED' as COMPLETED, 'FAILED' as FAILED, 'CANCELED' as CANCELED)
)
order by startdate desc
