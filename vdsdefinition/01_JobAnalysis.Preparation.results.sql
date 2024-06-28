CREATE OR REPLACE VDS 
JobAnalysis.Preparation.results 
AS
SELECT
    job_id AS queryId,
    status AS outcome,
    query_type AS queryType,
    user_name AS username,
    queried_datasets AS parentsList,
    scanned_datasets AS scannedDatasets,
    CAST(NULL AS INT) AS executionCpuTimeNs,
    attempt_count AS attemptCount,
    TO_TIMESTAMP(submitted_ts, 'YYYY-MM-DD HH24:MI:SS.FFF', 1) AS startTime,
    TO_TIMESTAMP(attempt_started_ts, 'YYYY-MM-DD HH24:MI:SS.FFF', 1) AS attempt_started_ts,
    TO_TIMESTAMP(metadata_retrieval_ts, 'YYYY-MM-DD HH24:MI:SS.FFF', 1) AS metadata_retrieval_ts,
    TO_TIMESTAMP(planning_start_ts, 'YYYY-MM-DD HH24:MI:SS.FFF', 1) AS planning_start_ts,
    TO_TIMESTAMP(query_enqueued_ts, 'YYYY-MM-DD HH24:MI:SS.FFF', 1) AS query_enqueued_ts,
    TO_TIMESTAMP(engine_start_ts, 'YYYY-MM-DD HH24:MI:SS.FFF', 1) AS engine_start_ts,
    TO_TIMESTAMP(execution_planning_ts, 'YYYY-MM-DD HH24:MI:SS.FFF', 1) AS execution_planning_start_ts,
    TO_TIMESTAMP(execution_start_ts, 'YYYY-MM-DD HH24:MI:SS.FFF', 1) AS execution_start_ts,
    TO_TIMESTAMP(final_state_ts, 'YYYY-MM-DD HH24:MI:SS.FFF', 1) AS finishTime,
    (final_state_epoch_millis - attempt_started_epoch_millis) AS "totalDurationMS",
    submitted_epoch_millis AS submitted,
    metadata_retrieval_epoch_millis AS metadataRetrieval,
    planning_start_epoch_millis AS planningStart,
    query_enqueued_epoch_millis AS queryEnqueued,
    execution_planning_ts AS executionPlanningStart,
    execution_start_epoch_millis AS executionStart,
    final_state_epoch_millis,
    planner_estimated_cost AS queryCost,
    rows_scanned AS inputRecords,
    bytes_scanned AS inputBytes,
    rows_returned AS outputRecords,
    bytes_returned AS outputBytes,
    accelerated,
    queue_name AS queueName,
    engine AS engineName,
    CONVERT_FROM('[]', 'JSON') AS executionNodes,
    CAST(NULL AS INT) AS memoryAllocated,
    error_msg AS outcomeReason,
    query AS queryText,
    CAST(NULL AS VARCHAR) AS nrQueryChunks,
    CONVERT_FROM('[]', 'JSON') AS reflectionRelationships,
    TO_TIMESTAMP(attempt_started_ts, 'YYYY-MM-DD HH24:MI:SS.FFF', 1) AS starting_ts,
    query AS queryTextFirstChunk,
    CAST(NULL AS VARCHAR) queryChunkSizeBytes,
    CAST(NULL AS VARCHAR) context,
    CAST(NULL AS VARCHAR) requestType,
    CASE WHEN metadata_retrieval_epoch_millis = 0 then 0
        ELSE (metadata_retrieval_epoch_millis - attempt_started_epoch_millis) end as poolWaitTime,
    CASE WHEN (engine_start_epoch_millis  = 0 or planning_start_epoch_millis = 0 ) then 0
        ELSE (engine_start_epoch_millis  - planning_start_epoch_millis ) end as planningTime,
    CASE WHEN query_enqueued_epoch_millis  = 0  then 0
        ELSE (query_enqueued_epoch_millis  - engine_start_epoch_millis ) end as engineStartTime,
    CASE WHEN execution_planning_epoch_millis  = 0 then 0
        ELSE (execution_planning_epoch_millis  - query_enqueued_epoch_millis ) end as enqueuedTime,
    CASE WHEN ( execution_start_epoch_millis  = 0  or execution_planning_epoch_millis = 0 )then 0
        ELSE (execution_start_epoch_millis  - execution_planning_epoch_millis ) end as executionPlanningTime,
    CASE WHEN ( final_state_epoch_millis  = 0 or execution_start_epoch_millis = 0 ) then 0
        ELSE (final_state_epoch_millis  - execution_start_epoch_millis ) end as executionTime,

    CAST(NULL AS VARCHAR) pendingTime,
    CAST(NULL AS VARCHAR) metadataRetrievalTime,
    CAST(NULL AS VARCHAR) startingTime,
    CAST(NULL AS VARCHAR) runningTime,
    CAST(NULL AS VARCHAR) setupTimeNs,
    CAST(NULL AS VARCHAR) waitTimeNs,
    CAST(NULL AS VARCHAR) startingStart,
    CAST(NULL AS VARCHAR) isTruncatedQueryText,
    CAST(NULL AS VARCHAR) materializationFor,
    CAST(NULL AS VARCHAR) outcomereasonshort
from sys.jobs_recent

