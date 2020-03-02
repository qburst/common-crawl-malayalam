# common-crawl-malayalam  
Useful tools for extracting malayalam text from the Common Crawl Dataset  
  
### Running on AWS  
* AWS ATHENA can be used to query the cc index table  to get the offsets of malayalam content text in a CSV file.  
  The query results would be available in S3

```  
-- For malayalam primary / secondary of January 2020 --  
-- If only primary is needed, use content_language LIKE 'mal%' --  
  
SELECT url,  
 warc_filename, warc_record_offset, warc_record_lengthFROM "ccindex"."ccindex"  
WHERE (crawl = 'CC-MAIN-2020-05 ')  
 AND subset = 'warc' AND content_languages LIKE '%mal%'
```  

  
  
* After you have the csv, an AWS EMR cluster can be spun up to retrieve the records from WARC files hosted in S3. 
The spark-submit job can be submitted using the following paramenters.
The app JAR is available in the repo (artifacts/cc-index-table-0.2-SNAPSHOT-jar-with-dependencies.jar)\
or you can build it from [source](https://github.com/commoncrawl/cc-index-table) using maven.  
  
```  
> spark-submit --class org.commoncrawl.spark.examples.CCIndexWarcExport $APPJAR \ 
--csv s3://<input_csv_path> --numOutputPartitions 24 --numRecordsPerWarcFile -1 \
--warcPrefix MALAYALAM-CC-2020-05 s3://commoncrawl/cc-index/table/cc-main/warc/  s3://<output_path>  
```  
  
* The spark job writes the extracted WARC files back to s3 and will be available at s3://<output_path>.   
  
* For cleaning the extracted html content, a library such as **selectolax** can be used. A reference script is available at [src/process_warc_batch.py].  
  
* The script can be run from an EC2 instance. The files can be then be compressed and written back to s3 and downloaded to your local machine.  
  
#### Usage 
We are including a couple of scripts to easily excecute these commands in AWS. All you have to provide is the
AWS credentials and the path of the S3 bucket.

* First, run extract_malayalam_warcs.sh as follows:
```
./extract_malayalam_warcs.sh <AWS Secret Key ID> <AWS Secret Key> <Crawl ID> <S3 Bucket Root Path> <Previously processed csv>

Example:-
./extract_malayalam_warcs.sh AK1232CJGTCWBMM312 YJDohI_6ADlvs4iv323RsJ2_MOFUUNjCZYB3kQgVN 2019-51 s3://nlp-malayalam 2020-05.csv
```   
* After you run the above script, you would have the WARC files in S3.
* In order to extract the malayalam content, you can either download the WARC files back to your machine or spin up an
EC2 instance and process it there.
* We are also providing a handy script to extract malayalam content in filter_malayalam.sh. It's usage is as follows:-
```buildoutcfg
./filter_malayalam.sh <AWS Secret Key ID> <AWS Secret Key> <S3 Warc Folder> <S3 Out Directory>
```  
  
  
  
#### Public Dataset 
Please find the cleaned up Malayalam text from the Common Crawl Archives Below:  
  

------------------------------------------------------------------------
| Common Crawl Date | Link to Cleaned Dataset                          |
|-------------------|--------------------------------------------------|
| 2020-05 	    | https://calicut.qburst.in/commoncrawl/malayalam/ |
------------------------------------------------------------------------

