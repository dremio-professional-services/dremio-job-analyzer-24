CREATE OR REPLACE VDS 
JobAnalysis.Application.Summary.Top20ExecutionTimes  
AS 
SELECT * FROM JobAnalysis.Business.SelectQueryData 
WHERE executionTime IS NOT NULL 
ORDER BY executionTime DESC 
LIMIT 20
