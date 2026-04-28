# Dremio Job Analyzer - VDS Creator
Job Analyzer is a package of VDS definitions which can be created over the sys.jobs_recent system table to analyze the jobs that have been processed in Dremio Software.

## Installation
On the command line, navigate to /path_to_dremio-job-analyzer-24/src.

Execute the following command:
```
python vds-creator.py [-h] --url URL --user USER --password PASSWORD  --vds-def-dir VDS_DEF_DIR [--tls-verify]
```

e.g.
```
python3 vds-creator.py --url http://localhost:9047 --user admin --password abcd1234 --vds-def-dir ../vdsdefinition
```

## Useful Queries for v26 and up.
sys.jobs_recent by default holds only the last 30 days of queries.  If you want to keep a history longer than 30 days, you can create an Iceberg table in you Open Catalog.  Here are some useful queries.


#Create your base table in Open Catalog...

under open catalog create the ```JobHistory``` folder

then run this query.

```
create table OpenCatalog.JobHistory.jobs as 
select current_timestamp() rec_insert_ts,*
From sys.jobs_recent where final_state_ts > '1970-01-01'
```

To update the table with recent data, run this query...

```
insert into OpenCatalog.JobHistory.jobs select current_timestamp() rec_insert_ts,* From sys.jobs_recent
where final_state_ts > (select COALESCE(max(final_state_ts),0) from OpenCatalog.JobHistory.jobs)"}
```

To retrieve all jobs...
```
select * from (
    select * From sys.jobs_recent  -- order by submitted_ts desc
    where submitted_ts > current_date()
    UNION ALL
    select * from OpenCatalog.JobHistory.jobs
) 
order by submitted_ts desc
```

In a K8s envirnment, you can create a cronjob which runs the incremental query every 4 hrs.  Use this file as a template:

```
apiVersion: batch/v1
kind: CronJob
metadata:
  name: jrun
#  namespace: dremio  # same namespace as your Dremio deployment
spec:
  schedule: "0 0,6,12,18 * * *"  # Every 1 hours
  concurrencyPolicy: Forbid
  jobTemplate:
    spec:
      backoffLimit: 2
      template:
        spec:
          nodeSelector:
            karpenter.sh/nodepool: dremio-common  # use your common/utility nodepool
          restartPolicy: OnFailure
          containers:
            - name: jrun
              image: curlimages/curl:latest
#              command: ["/bin/sh", "-c"]
#              args: ["sleep infinity"]   # For Testing
              command:
                - /bin/sh
                - -c
                - |
                  # 1. Authenticate and get a token
                  TOKEN=$(curl -s -X POST \
                    "http://dremio-client:443/apiv2/login" \
                    -H "Content-Type: application/json" \
                    -d '{"userName":"'$DREMIO_USER'","password":"'$DREMIO_PASS'"}' \
                    | grep -o '"token":"[^"]*"' | cut -d'"' -f4)

                  if [ -z "$TOKEN" ]; then
                    echo "ERROR: Failed to authenticate"
                    exit 1
                  fi
                  # echo $TOKEN  #enable for Debugging
                  # 2. Submit the SQL query
                  JOB=$(curl -s -X POST "http://dremio-client:443/api/v3/sql" -H "Content-Type: application/json" -H "Authorization: _dremio$TOKEN" -d  '{"sql":"insert into '$JOB_HIST_PATH'.jobs select current_timestamp() rec_insert_ts,* From sys.jobs_recent where final_state_ts > (select COALESCE(max(final_state_ts),0) from '$JOB_HIST_PATH'.jobs)"}' )
                  echo "Job submitted: $JOB"

                  # 3. Optionally poll for completion
                  JOB_ID=$(echo "$JOB" | grep -o '"id":"[^"]*"' | cut -d'"' -f4)
                  STATUS="RUNNING"
                  while [ "$STATUS" = "RUNNING" ] || [ "$STATUS" = "STARTING" ] || [ "$STATUS" = "ENQUEUED" ] || [ "$STATUS" = "ENGINE_START" ] ; do
                    sleep 30
                    RESULT=$(curl -s \
                      "http://dremio-client:443/api/v3/job/$JOB_ID" \
                      -H "Authorization: Bearer $TOKEN")
                    STATUS=$(echo "$RESULT" | grep -o '"jobState":"[^"]*"' | cut -d'"' -f4)
                    echo "Job $JOB_ID status: $STATUS"
                  done

                  if [ "$STATUS" != "COMPLETED" ]; then
                    echo "ERROR: Job finished with status $STATUS"
                    exit 1
                  fi
                  echo "Job completed successfully"
              env:
                - name: DREMIO_USER
                  valueFrom:
                    secretKeyRef:
                      name: dremio-query-credentials
                      key: username
                - name: DREMIO_PASS
                  valueFrom:
                    secretKeyRef:
                      name: dremio-query-credentials
                      key: password
                - name: JOB_HIST_PATH
                  value: "MazOC.internal.history"

# Use This command to set the secrets
#kubectl create secret generic dremio-query-credentials \
#  --namespace dremio \
#  --from-literal=username=your_dremio_user \
#  --from-literal=password=your_pat_token
```
