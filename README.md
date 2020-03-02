# common-crawl-malayalam
Useful tools to extract malayalam text from the Common Crawl Datasets


*  Running Athena query to get the offsets of malayalam content text.

```
-- For malayalam primary / secondary of January 2020 --
-- If only primary is needed, use content_language LIKE 'mal%' --

SELECT url,
       warc_filename,
       warc_record_offset,
       warc_record_length
FROM "ccindex"."ccindex"
WHERE (crawl = 'CC-MAIN-2020-05 ')
  AND subset = 'warc'
  AND content_languages LIKE '%mal%'
```


*  Then, setup spark and run the below commands using spark-submit. The app jar is available from the link[] or you can build it from [source](https://github.com/commoncrawl/cc-index-table) using maven.


```

> spark-submit --class org.commoncrawl.spark.examples.CCIndexWarcExport $APPJAR

Spark submit arguments 
--class org.commoncrawl.spark.examples.CCIndexWarcExport 

App arguments 
--csv s3://<input_csv_path> --numOutputPartitions 24 --numRecordsPerWarcFile -1 \  
--warcPrefix MALAYALAM-CC-2020-05 s3://commoncrawl/cc-index/table/cc-main/warc/  s3://<output_path>
```

* The spark job writes the results back to s3 and will be available at s3://<output_path>. 

* For cleaning the extracted html content, a library such as **selectolax** can be used. A reference script is available at :(GITHUB link here).

* The script can be run from an EC2 instance. The files can be then be compressed and written back to s3 and downloaded to your local machine.

