# Routing Project

Giving historical airplane on time performance data, offer suggestions for two-hop flights that minimize the chance of missing a connection.

## Code Structure

- `Driver`  : `org.neu.RoutePrediction`
  - Arguments `<iterations> <queryFile> <input> <output> <report-loc> <training-year-length>`
- `Job` :  `org.neu.job.RouteComputeJob`

## Hadoop Cluster Config
### Pseudo-distributed
Used with Experiment 1 mentioned in the report
- `conf/pseudo-distributed/core-site.xml`
- `conf/pseudo-distributed/hdfs-site.xml`
- `conf/pseudo-distributed/mapred-site.xml`
- `conf/pseudo-distributed/yarn-site.xml`

## Running Instructions

### Local

#### 1. Prepare

1) Create your query with format: "YYYY, MM, DD, SOURCE_AIRPORT, DESTINATION_AIRPORT" (e.g. 2001, 09, 11, DEN, DCA). Put your queries in `query/query.csv`.

2) Put your Input Files(flight csv data) at `input/all`.

3) Modify `HADOOP_HOME` and `HADOOP_VERSION` in `Makefile` to your hadoop home and version.

#### 2. Run 

- Goto `<project-root>`
- Run `make`

### AWS EMR
Make sure you have AWS CLI working with your KEY+SECRET
#### 1. Prepare

```
make setup-s3
```

#### 2. Run 

```
make cloud AWS_REGION=us-east-1 AWS_BUCKET_NAME=mr-neighbor AWS_SUBNET_ID=subnet-51e4fd7a AWS_NUM_NODES=1 AWS_INSTANCE_TYPE=m1.medium INPUT_TYPE=books AWS_NUM_NODES=1
```

## Reports
- Score : `make get-output-row-count (gives o/p count. Use 'hdfs dfs -get output output' to copy all output files to local)`
- R Markdown Report : `report/report.Rmd`
- HTML Report : `report/report.html`
- PDF Report : `report/report.pdf`
- Input Queries : `query/query.csv`
- Output Routes : `report/finalOutputRoutes.csv`
