create or replace VDS JobAnalysis.Application.DailySummary.DailyReflectionKPIReport as 

WITH DailyReflectionbyOutcome as (
    select *,COMPLETED+FAILED+CANCELED as TOTAL from (
        SELECT cast(startTime as date) startdate,outcome FROM JobAnalysis.Business.ReflectionRefreshData
    )
    PIVOT (
        count(outcome) for "outcome" in ('COMPLETED' as COMPLETED, 'FAILED' as FAILED, 'CANCELED' as CANCELED)
    )
    order by startdate
),
DailyReflectionKPI as (
select cast(startTime as date) sdate, count(*) as cnt, 
    cast(avg(enqueuedTime)/1000 as int) as avgQueueTimeSec, 
    cast(avg(executionTime)/1000 as int) avgExecTimeSec , 
    cast(avg(totalDurationSeconds) as int) avgTotalDurationSec ,
    cast(avg(memoryAllocated)/1000000 as int) memAllocMB
from JobAnalysis.Business.ReflectionRefreshData 
-- where outcome='COMPLETED'
group by 1 order by 1
)

select startdate,COMPLETED,FAILED,CANCELED,TOTAL,
    avgQueueTimeSec,avgExecTimeSec,
    avgTotalDurationSec,memAllocMB
from DailyReflectionbyOutcome  dro
left join DailyReflectionKPI dsp on dro.startdate =dsp.sdate
order by startdate desc
