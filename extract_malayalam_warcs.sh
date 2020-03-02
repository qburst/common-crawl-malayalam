#!/bin/bash
# Prerequisite packages : python3

printf "\nArguments \n"
printf "========================================================================\n"
echo "AWS Key ID:                  " $1
echo "AWS Secret Key:              " $2
echo "CC crawl id:                 " $3
echo "S3 Bucket Root Path:         " $4
echo "Previously crawled csv file: " $5
printf "========================================================================\n"

aws configure set aws_access_key_id $1
aws configure set aws_secret_access_key $2

CC_CRAWL_ID=$3
S3_BUCKET=$4
OLD_CSV=$5

# Wait for the query to finish running.
# This will wait for up to 60 seconds (30 * 2)
wait_for_query_execution_status() {
  for i in $(seq 1 30); do
    queryState=$(
      aws athena get-query-execution --query-execution-id "$1" --region us-east-1 | python3 json_parser.py ".QueryExecution.Status.State"
    )

    if [[ "${queryState}" == "SUCCEEDED" ]]; then
      return 0
    fi

    echo "Awaiting queryExecutionId $1 - state was ${queryState}"

    if [[ "${queryState}" == "FAILED" ]]; then
      # return with "bad" error code
      return 1
    fi
    sleep 2
  done
  return 1
}

echo "Creating Database ccindex in Athena"
queryExecutionId=$(
  aws athena start-query-execution \
    --query-string "CREATE DATABASE ccindex" \
    --result-configuration OutputLocation=s3://nlp-malayalam/meta_queries/ \
    --region us-east-1 | python3 json_parser.py ".QueryExecutionId"
)

echo "queryExecutionId was $queryExecutionId"

wait_for_query_execution_status $queryExecutionId
result=$?

if [ $result -eq 1 ]; then
  echo "Database creation failed"
  echo "Database might already exist. Continuing ..."
else
  echo "Creating database ccindex was successful."
fi

printf "\nCreating ccindex table in the database\n"
queryExecutionId=$(
  aws athena start-query-execution --query-string "CREATE EXTERNAL TABLE IF NOT EXISTS ccindex.ccindex (
         url_surtkey STRING,
         url STRING,
         url_host_name STRING,
         url_host_tld STRING,
         url_host_2nd_last_part STRING,
         url_host_3rd_last_part STRING,
         url_host_4th_last_part STRING,
         url_host_5th_last_part STRING,
         url_host_registry_suffix STRING,
         url_host_registered_domain STRING,
         url_host_private_suffix STRING,
         url_host_private_domain STRING,
         url_protocol STRING,
         url_port INT,
         url_path STRING,
         url_query STRING,
         fetch_time TIMESTAMP,
         fetch_status SMALLINT,
         fetch_redirect STRING,
         content_digest STRING,
         content_mime_type STRING,
         content_mime_detected STRING,
         content_charset STRING,
         content_languages STRING,
         content_truncated STRING,
         warc_filename STRING,
         warc_record_offset INT,
         warc_record_length INT,
         warc_segment STRING
) PARTITIONED BY (
         crawl STRING,
         subset STRING
) STORED AS parquet LOCATION 's3://commoncrawl/cc-index/table/cc-main/warc/'" --result-configuration OutputLocation=${4}/meta_queries/ | python3 json_parser.py ".QueryExecutionId"
)

echo "queryExecutionId was $queryExecutionId"

wait_for_query_execution_status $queryExecutionId
result=$?

if [ $result -eq 1 ]; then
  echo "Table creation failed"
  echo "Exiting ..."
  exit 1
fi
echo "Table Creation succeeded"

printf "\nRepairing Table and recovering Partitions\n"
queryExecutionId=$(
  aws athena start-query-execution --query-string "MSCK REPAIR TABLE ccindex.ccindex" --result-configuration OutputLocation=${4}/meta_queries/ | python3 json_parser.py ".QueryExecutionId"
)

echo "queryExecutionId was $queryExecutionId"

wait_for_query_execution_status $queryExecutionId
result=$?

if [ $result -eq 1 ]; then
  echo "Index Repair failed"
  echo "Exiting ..."
  exit 1
fi
echo "Table partition recovery successful"

printf "\n\nStarting with common crawl index scan for Malayalam\n"
queryExecutionId=$(
  aws athena start-query-execution --query-string "SELECT url, warc_filename, warc_record_offset, warc_record_length
FROM ccindex.ccindex WHERE (crawl = 'CC-MAIN-$CC_CRAWL_ID') AND subset = 'warc'
AND content_languages LIKE 'mal%'" --result-configuration OutputLocation=$S3_BUCKET/$CC_CRAWL_ID/ | python3 json_parser.py ".QueryExecutionId"
)

echo "queryExecutionId was $queryExecutionId"
wait_for_query_execution_status $queryExecutionId
result=$?

if [ $result -eq 1 ]; then
  echo "Scan failed"
  echo "Exiting ..."
  exit 1
fi

CSV_FILENAME="$queryExecutionId.csv"
S3_CSV_PATH="$S3_BUCKET/$CC_CRAWL_ID/${CSV_FILENAME}"
echo "Query Complete. CSV results availabe at path : $S3_CSV_PATH"

printf "\n\nCopying csv file from s3 to local for filtering out duplicates.\n"
aws s3 cp $S3_CSV_PATH ./$CC_CRAWL_ID.csv
python3 remove_duplicates.py ./$CC_CRAWL_ID.csv $OLD_CSV ./"$CC_CRAWL_ID"_filtered_out.csv
aws s3 cp ./"$CC_CRAWL_ID"_filtered_out.csv $S3_BUCKET/$CC_CRAWL_ID/"$CC_CRAWL_ID"_filtered_out.csv

echo "Filtering complete. Uploading results back to S3"

# APP_JAR_PATH is provided from our repo as cc-index-commoncrawl repo has a bug with EMR and needs to be rebuild after editing source
printf "\n\nUploading spark app jar to s3.\n"
aws s3 cp ./cc-index-table-0.2-SNAPSHOT-jar-with-dependencies.jar "$S3_BUCKET/cc-index-table-0.2-SNAPSHOT-jar-with-dependencies.jar"

APP_JAR_PATH="$S3_BUCKET/cc-index-table-0.2-SNAPSHOT-jar-with-dependencies.jar"
CSV_PATH=$S3_BUCKET/$CC_CRAWL_ID/"$CC_CRAWL_ID"_filtered_out.csv
OUTPUT_DIRECTORY="$S3_BUCKET/$CC_CRAWL_ID/warcs/"
LOG_DIRECTORY="$S3_BUCKET/$CC_CRAWL_ID/emr-logs/"
WARC_PREFIX="MALAYALAM-CC-$CC_CRAWL_ID"

clusterId=$(
  aws emr create-cluster \
    --release-label emr-5.29.0 \
    --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m5.xlarge InstanceGroupType=CORE,InstanceCount=2,InstanceType=m5.xlarge \
    --use-default-roles \
    --applications Name=JupyterHub Name=Spark Name=Hadoop \
    --name=CommonCrawlMalayalamCluster \
    --log-uri "$LOG_DIRECTORY" \
    --steps '[{"Args":["spark-submit","--deploy-mode","cluster","--class","org.commoncrawl.spark.examples.CCIndexWarcExport", "'$APP_JAR_PATH'","--csv","'$CSV_PATH'","--numOutputPartitions","20","--numRecordsPerWarcFile","-1", "--warcPrefix","'$WARC_PREFIX'","s3://commoncrawl/cc-index/table/cc-main/warc/","'$OUTPUT_DIRECTORY'"], "Type":"CUSTOM_JAR","ActionOnFailure":"TERMINATE_CLUSTER","Jar":"command-runner.jar","Properties":"","Name":"CC Warc fetch"}]' \
    --region us-east-1 \
    --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
    --auto-terminate | python3 json_parser.py ".ClusterId"
)

echo "Please Check $OUTPUT_DIRECTORY in S3 for completed files. It should take 4-5 hours to complete"
echo "Exiting"