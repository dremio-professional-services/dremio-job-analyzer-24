# Dremio Job Analyzer - VDS Creator
Job Analyzer is a package of VDS definitions which can be created over the sys.jobs_recent system table to analyze the jobs that have been processed in Dremio Software.

## Usage
On the command line, navigate to /path_to_dremio-job-analyzer-24/src.

Execute the following command:
```
python vds-creator.py [-h] --url URL --user USER --password PASSWORD  --vds-def-dir VDS_DEF_DIR [--tls-verify]
```

e.g.
```
python3 vds-creator.py --url http://localhost:9047 --user admin --password abcd1234 --vds-def-dir ../vdsdefinition
```
