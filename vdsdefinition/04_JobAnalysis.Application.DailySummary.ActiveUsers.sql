CREATE OR REPLACE VDS 
JobAnalysis.Application.DailySummary.ActiveUsers 
AS 
SELECT 
	TO_DATE("startTime") queryStartDate, 
	username, 
	COUNT(*) totalQueries, 
	SUM(totalDurationMS) totalWallclockMS 
FROM JobAnalysis.Business.SelectQueryData 
GROUP BY username, queryStartDate 
ORDER BY queryStartDate DESC, totalQueries DESC, totalWallclockMS DESC
