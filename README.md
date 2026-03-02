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
select * From sys.jobs_recent
where submitted_ts < 'Some_date in the past 30 days'
```

To update the table with recent data, run this query...

```
insert into OpenCatalog.JobHistory.jobs
select * From sys.jobs_recent
where submitted_ts > (select max(submitted_ts) from OpenCatalog.JobHistory.jobs) and submitted_ts < CURRENT_DATE()
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
