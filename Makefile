REPETITIONS=1
TRAINING_YR_LENGTH=1
HADOOP_HOME=/usr/local/hadoop
HADOOP_VERSION=2.8.1
MY_CLASSPATH=${HADOOP_HOME}/share/hadoop/common/hadoop-common-${HADOOP_VERSION}.jar:${HADOOP_HOME}/share/hadoop/mapreduce/*:lib/*:lib/commons-lang3-3.6/*:lib/commons-lang-2.6/*:out:.
PROJECT_BASE=src/main/java/org/neu
INPUT_FOLDER=input
QUERY_FOLDER=query
OUTPUT_FOLDER=output
# INPUT_TYPE is the folder inside input folder. you can define multiple folders contining diff no. of inputs
INPUT_TYPE=all
REPORT_FOLDER=report
JAR_NAME=RoutePrediction.jar
JAR_PATH=${JAR_NAME}
JOB_NAME=RoutePrediction

# AWS EMR Execution
# Inspired from MAKE Scripts authored by
#  - Jan Vitek -> http://www.ccis.northeastern.edu/people/jan-vitek/
#  - Joseph Sackett -> http://www.ccis.northeastern.edu/people/joseph-sackett/
AWS_EMR_RELEASE=emr-5.8.0
AWS_REGION=us-east-1
AWS_BUCKET_NAME=mr-routeprediction
AWS_SUBNET_ID=subnet-51e4fd7a
AWS_INPUT=${INPUT_FOLDER}
AWS_OUTPUT=${OUTPUT_FOLDER}
AWS_CONFIG=config/config.json
AWS_LOG_DIR=log
AWS_NUM_NODES=1
AWS_INSTANCE_TYPE=m1.medium

all: build setup-hdfs run

all-uz: build setup-hdfs run

build: compile jar

compile:
	mkdir -p out
	javac -cp ${MY_CLASSPATH} -d out \
	${PROJECT_BASE}/*.java \
	${PROJECT_BASE}/util/*.java \
	${PROJECT_BASE}/job/*.java \
	${PROJECT_BASE}/mapper/*.java \
	${PROJECT_BASE}/reducer/*.java \
	${PROJECT_BASE}/data/*.java

jar:
	cp -r META-INF/MANIFEST.MF out
	cd out; jar cvmf MANIFEST.MF ${JAR_NAME} * ../lib
	mv out/${JAR_NAME} .

run:
	${HADOOP_HOME}/bin/hadoop jar ${JAR_NAME} ${REPETITIONS} ${QUERY_FOLDER} ${INPUT_FOLDER} ${OUTPUT_FOLDER} ${REPORT_FOLDER} ${TRAINING_YR_LENGTH}

clean:
	$(HADOOP_HOME)/bin/hdfs dfs -rm -r output;

setup-hdfs:
	$(HADOOP_HOME)/bin/hdfs dfs -rm -r -f ${INPUT_FOLDER}
	$(HADOOP_HOME)/bin/hdfs dfs -rm -r -f ${QUERY_FOLDER}
	$(HADOOP_HOME)/bin/hdfs dfs -rm -r -f ${OUTPUT_FOLDER}
	$(HADOOP_HOME)/bin/hdfs dfs -put ${INPUT_FOLDER}/${INPUT_TYPE} ${INPUT_FOLDER}
	$(HADOOP_HOME)/bin/hdfs dfs -put ${QUERY_FOLDER} ${QUERY_FOLDER}

gzip:
	gzip ${INPUT_FOLDER}/${INPUT_TYPE}/*

gunzip:
	gunzip ${INPUT_FOLDER}/${INPUT_TYPE}/*

get-output-row-count:
	${HADOOP_HOME}/bin/hdfs dfs -cat ${OUTPUT_FOLDER}/knscore/part-r-00000 | wc -l

# Create S3 bucket.
make-bucket:
	aws s3 mb s3://${AWS_BUCKET_NAME}

# Setup S3
setup-s3: make-bucket upload-input-aws make-report-folder

# Upload data to S3 input dir.
upload-input-aws:
	aws s3 sync ${INPUT_FOLDER}/${INPUT_TYPE} s3://${AWS_BUCKET_NAME}/${AWS_INPUT}/${INPUT_TYPE}

# Setup Report
make-report-folder:
	aws s3 sync ${REPORT_FOLDER} s3://${AWS_BUCKET_NAME}/${REPORT_FOLDER} --exclude "*"

# Delete S3 output dir.
delete-output-aws:
	aws s3 rm s3://${AWS_BUCKET_NAME}/ --recursive --exclude "*" --include "${AWS_OUTPUT}*"

# Upload application to S3 bucket.
upload-app-aws:
	aws s3 cp ${JAR_PATH} s3://${AWS_BUCKET_NAME}

# Main EMR launch.
cloud: build upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name "Flight Performance" \
		--release-label ${AWS_EMR_RELEASE} \
		--instance-groups InstanceCount=${AWS_NUM_NODES},InstanceGroupType=CORE,InstanceType=${AWS_INSTANCE_TYPE} InstanceCount=1,InstanceGroupType=MASTER,InstanceType=${AWS_INSTANCE_TYPE} \
	    --applications Name=Hadoop \
	    --steps Args=${REPETITIONS},${INPUT_QUERY},s3://${AWS_BUCKET_NAME}/${AWS_INPUT}/${INPUT_TYPE},s3://${AWS_BUCKET_NAME}/${AWS_OUTPUT},s3://${AWS_BUCKET_NAME}/${REPORT_FOLDER},${TRAINING_YR_LENGTH},Type=CUSTOM_JAR,Jar=s3://${AWS_BUCKET_NAME}/${JAR_NAME},ActionOnFailure=TERMINATE_CLUSTER,Name=${JOB_NAME} \
		--log-uri s3://${AWS_BUCKET_NAME}/${AWS_LOG_DIR} \
		--service-role EMR_DefaultRole \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${AWS_SUBNET_ID} \
		--region ${AWS_REGION} \
		--enable-debugging \
		--auto-terminate

cloud-custom: build upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name "RoutePrediction" \
		--release-label ${AWS_EMR_RELEASE} \
		--instance-groups InstanceCount=${AWS_NUM_NODES},InstanceGroupType=CORE,InstanceType=${AWS_INSTANCE_TYPE} InstanceCount=1,InstanceGroupType=MASTER,InstanceType=${AWS_INSTANCE_TYPE} \
	    --applications Name=Hadoop \
	    --steps Args=${REPETITIONS},${INPUT_QUERY},s3://${AWS_BUCKET_NAME}/${AWS_INPUT}/${INPUT_TYPE},s3://${AWS_BUCKET_NAME}/${AWS_OUTPUT},s3://${AWS_BUCKET_NAME}/${REPORT_FOLDER},${TRAINING_YR_LENGTH},Type=CUSTOM_JAR,Jar=s3://${AWS_BUCKET_NAME}/${JAR_NAME},ActionOnFailure=TERMINATE_CLUSTER,Name=${JOB_NAME} \
		--log-uri s3://${AWS_BUCKET_NAME}/${AWS_LOG_DIR} \
		--service-role EMR_DefaultRole \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${AWS_SUBNET_ID} \
		--region ${AWS_REGION} \
		--enable-debugging \
		--auto-terminate \
		--configurations file://conf/aws/config.json

cloud-big: build upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name "RoutePrediction" \
		--release-label ${AWS_EMR_RELEASE} \
		--instance-groups InstanceCount=${AWS_NUM_NODES},InstanceGroupType=CORE,InstanceType=${AWS_INSTANCE_TYPE} InstanceCount=1,InstanceGroupType=MASTER,InstanceType=m4.large \
	    --applications Name=Hadoop \
	    --steps Args=${REPETITIONS},${INPUT_QUERY},s3://${AWS_BUCKET_NAME}/${AWS_INPUT}/${INPUT_TYPE},s3://${AWS_BUCKET_NAME}/${AWS_OUTPUT},s3://${AWS_BUCKET_NAME}/${REPORT_FOLDER},${TRAINING_YR_LENGTH},Type=CUSTOM_JAR,Jar=s3://${AWS_BUCKET_NAME}/${JAR_NAME},ActionOnFailure=TERMINATE_CLUSTER,Name=${JOB_NAME} \
		--log-uri s3://${AWS_BUCKET_NAME}/${AWS_LOG_DIR} \
		--service-role EMR_DefaultRole \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${AWS_SUBNET_ID} \
		--region ${AWS_REGION} \
		--enable-debugging \
		--auto-terminate

# Download output from S3.
download-output-aws: clean-local-output
	mkdir ${OUTPUT_FOLDER}
	aws s3 sync s3://${AWS_BUCKET_NAME}/${AWS_OUTPUT} ${OUTPUT_FOLDER}

# Removes local output directory.
clean-local-output:
	rm -rf ${OUTPUT_FOLDER}*