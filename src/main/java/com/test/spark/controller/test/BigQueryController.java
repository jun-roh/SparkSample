package com.test.spark.controller.test;

import com.google.api.services.bigquery.model.JobStatistics2;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.test.spark.config.GCP.GCPConfig;
import com.test.spark.util.FileUtil;
import com.test.spark.util.JsonUtil;
import com.test.spark.util.RedisUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.boot.configurationprocessor.json.JSONArray;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Controller
@RequestMapping("/")
public class BigQueryController {
    private JavaSparkContext sparkContext;
    private SparkSession sparkSession;
    private JavaStreamingContext streamingContext;
    private RedisUtil redisUtil;
    private GCPConfig GCPConfig;
    private FileUtil fileUtil;

    public BigQueryController(JavaSparkContext sparkContext, SparkSession sparkSession, JavaStreamingContext streamingContext,
                              RedisUtil redisUtil, FileUtil fileUtil, GCPConfig GCPConfig){
        this.sparkContext = sparkContext;
        this.sparkSession = sparkSession;
        this.streamingContext = streamingContext;
        this.redisUtil = redisUtil;
        this.fileUtil = fileUtil;
        this.GCPConfig = GCPConfig;
    }

    @GetMapping("/bigquery_spark")
    public String bigQuerySpark(Model model){
        String key = "json_file";
        String path = "src/main/resources/temp/"+key+".json";
        String query = TestQuery.query_1;

        try {
            // redis key 존재하지 않으면 Bigquery 에서 데이터 가져와서 json 에 넣도록 함
            if (redisUtil.getValue(key) == null){
                long bigquery_before_time = System.currentTimeMillis();
                System.out.println("#### bigquery Start Time : " + bigquery_before_time + "####");

                // Bigquery Query 처리
                BigQuery bigQuery = GCPConfig.setBigquery();
                QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query)
                        .setJobTimeoutMs(60000L).setUseLegacySql(false).build();

                JobId jobId = JobId.of(UUID.randomUUID().toString());
                Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

                queryJob = queryJob.waitFor();

                if (queryJob == null) {
                    throw new RuntimeException("Job no longer exists");
                } else if (queryJob.getStatus().getError() != null) {
                    throw new RuntimeException(queryJob.getStatus().getError().toString());
                }

                TableResult result = queryJob.getQueryResults();

                long bigquery_after_time = System.currentTimeMillis();
                long bigquery_diff_time = (bigquery_after_time - bigquery_before_time) / 1000;
                System.out.println("#### bigquery End Time : " + bigquery_after_time + "####");
                System.out.println("#### bigquery Diff Time : " + bigquery_diff_time + "####");

                // Bigquery result -> json
                long json_before_time = System.currentTimeMillis();
                System.out.println("#### json Start Time : " + json_before_time + "####");
                List<String> fieldList = result.getSchema().getFields().stream()
                        .map(Field::getName)
                        .collect(Collectors.toList());
                JSONArray jsonArray = JsonUtil.convertTableResultToJson(result, fieldList);
                long json_after_time = System.currentTimeMillis();
                long json_diff_time = (json_after_time - json_before_time) / 1000;
                System.out.println("#### json End Time : " + json_after_time + "####");
                System.out.println("#### json Diff Time : " + json_diff_time + "####");

                // file 생성
                fileUtil.setFile(path, jsonArray);
                // redis 생성 (생명주기 부여)
                redisUtil.setValue(key, path, 1, TimeUnit.MINUTES);
            }

            long spark_before_time = System.currentTimeMillis();
            System.out.println("#### spark Start Time : " + spark_before_time + "####");
            // json 형태 데이터 -> spark dataset 변형
            Dataset<Row> df =  sparkSession.read().format("json")
                    .option("multiline", true)
                    .load(path);
            df.show();

            // dataset -> view 생성
            df.createOrReplaceTempView(key);
            long spark_after_time = System.currentTimeMillis();
            long spark_diff_time = (spark_after_time - spark_before_time) / 1000;
            System.out.println("#### spark End Time : " + spark_after_time + "####");
            System.out.println("#### spark Diff Time : " + spark_diff_time + "####");
            // view 에 원하는 쿼리를 통해서 spark 연산
            long spark_qry_before_time = System.currentTimeMillis();
            System.out.println("#### spark_qry Start Time : " + spark_qry_before_time + "####");
            List<String> df_string = sparkSession.sql("select _i_t, pt, max_story from "+key+" ")
                    .toJSON().collectAsList();

            long spark_qry_after_time = System.currentTimeMillis();
            long spark_qry_diff_time = (spark_qry_after_time - spark_qry_before_time) / 1000;
            System.out.println("#### spark End Time : " + spark_qry_after_time + "####");
            System.out.println("#### spark Diff Time : " + spark_qry_diff_time + "####");
        } catch (Exception e){
            System.out.println("-----");
            System.out.println(e.getMessage());
            System.out.println(Arrays.toString(e.getStackTrace()));
        }

        return "index";
    }

    @GetMapping("/bigquery_spark_json")
    public String bigQuerySparkJson(Model model){
        String key = "json_file";
        String path = "src/main/resources/temp/"+key+".json";
        String query = "SELECT TO_JSON_STRING(t) FROM ( \n" + TestQuery.query_2.replaceAll(";", "") + "\n ) AS t;";
        String query_field = "SELECT * FROM ( \n" + TestQuery.query_2.replaceAll(";", "") + "\n ) limit 1;";
        try {
            // redis key 존재하지 않으면 Bigquery 에서 데이터 가져와서 json 에 넣도록 함
            if (redisUtil.getValue(key) == null){
                long bigquery_before_time = System.currentTimeMillis();
                System.out.println("#### bigquery Start Time : " + bigquery_before_time + "####");

                // Bigquery Query 처리
                BigQuery bigQuery = GCPConfig.setBigquery();
                QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query)
                        .setJobTimeoutMs(60000L).setUseLegacySql(false).build();

                QueryJobConfiguration queryField = QueryJobConfiguration.newBuilder(query_field)
                        .setJobTimeoutMs(60000L).setUseLegacySql(false).build();

                JobId jobId = JobId.of(UUID.randomUUID().toString());
                Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

                JobId jobField = JobId.of(UUID.randomUUID().toString());
                Job queryJobField = bigQuery.create(JobInfo.newBuilder(queryField).setJobId(jobField).build());

                queryJob = queryJob.waitFor();
                queryJobField = queryJobField.waitFor();

                if (queryJob == null) {
                    throw new RuntimeException("Job no longer exists");
                } else if (queryJob.getStatus().getError() != null) {
                    throw new RuntimeException(queryJob.getStatus().getError().toString());
                }

                if (queryJobField == null) {
                    throw new RuntimeException("Job no longer exists");
                } else if (queryJobField.getStatus().getError() != null) {
                    throw new RuntimeException(queryJobField.getStatus().getError().toString());
                }

                TableResult result = queryJob.getQueryResults();
                TableResult resultField = queryJobField.getQueryResults();
                long bigquery_after_time = System.currentTimeMillis();
                long bigquery_diff_time = (bigquery_after_time - bigquery_before_time) / 1000;
                System.out.println("#### bigquery End Time : " + bigquery_after_time + "####");
                System.out.println("#### bigquery Diff Time : " + bigquery_diff_time + "####");

                // file 생성
                fileUtil.setFileFromTableResultJson(path, result);
                // redis 생성 (생명주기 부여)
                redisUtil.setValue(key, path, 5, TimeUnit.MINUTES);
            }

            long spark_before_time = System.currentTimeMillis();
            System.out.println("#### spark Start Time : " + spark_before_time + "####");
            // json 형태 데이터 -> spark dataset 변형
            Dataset<Row> df =  sparkSession.read().format("json")
                    .option("multiline", false)
                    .load(path);
            df.show();
            System.out.println("count : " + df.count());

            // dataset -> view 생성
            df.createOrReplaceTempView(key);
            long spark_after_time = System.currentTimeMillis();
            long spark_diff_time = (spark_after_time - spark_before_time) / 1000;
            System.out.println("#### spark End Time : " + spark_after_time + "####");
            System.out.println("#### spark Diff Time : " + spark_diff_time + "####");
            // view 에 원하는 쿼리를 통해서 spark 연산
            long spark_qry_before_time = System.currentTimeMillis();
            System.out.println("#### spark_qry Start Time : " + spark_qry_before_time + "####");
            List<String> df_string = sparkSession.sql("select _i_t, pt, max_story from "+key+" ")
                    .toJSON().collectAsList();

            df.unpersist();

            long spark_qry_after_time = System.currentTimeMillis();
            long spark_qry_diff_time = (spark_qry_after_time - spark_qry_before_time) / 1000;
            System.out.println("#### spark End Time : " + spark_qry_after_time + "####");
            System.out.println("#### spark Diff Time : " + spark_qry_diff_time + "####");
        } catch (Exception e){
            System.out.println("-----");
            System.out.println(e.getMessage());
            System.out.println(Arrays.toString(e.getStackTrace()));
        }

        return "index";
    }

    @GetMapping("/bigquery_storage")
    public String bigQueryStorage(Model model){

        String query = TestQuery.query_4;
        String sourceUri = "gs://spark-samples-data/sample/test_*.json";
        String dataFormat = "JSON";
        String key = "test";
        String sparkPath = "src/main/resources/temp/"+key+"_*.json";
        String storagePath = "sample/test_000000000000.json";
        String filePath = "src/main/resources/temp/"+key+"_000000000000.json";

        try {
            long bigquery_before_time = System.currentTimeMillis();
            System.out.println("#### bigquery Start Time : " + bigquery_before_time + "####");
            BigQuery bigQuery = GCPConfig.setBigquery();
            String qry =
                    String.format(
                            "EXPORT DATA OPTIONS(uri='%s', format='%s', overwrite=true) "
                                    + "AS \n %s",
                            sourceUri, dataFormat, query);

            String jsonQuery = "SELECT TO_JSON_STRING(t) FROM ( \n" + query.replaceAll(";", "") + "\n ) AS t;";

            QueryJobConfiguration queryConfig =
                    QueryJobConfiguration.newBuilder(query).setDryRun(true).setUseQueryCache(false).build();

            Job queryJob = bigQuery.create(JobInfo.of(queryConfig));
//
//            JobId jobId = JobId.of(UUID.randomUUID().toString());
//
//            Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
            // 비용이 들지 않음!
            JobStatistics.QueryStatistics statistics = queryJob.getStatistics();
            System.out.println("getEstimatedBytesProcessed :"+statistics.getEstimatedBytesProcessed());
            System.out.println("getTotalBytesProcessed :"+statistics.getTotalBytesProcessed());
            System.out.println("getBillingTier :"+statistics.getBillingTier());
            System.out.println("getNumDmlAffectedRows :"+statistics.getNumDmlAffectedRows());
            System.out.println("getTotalBytesBilled :"+statistics.getTotalBytesBilled());
            System.out.println("getTotalPartitionsProcessed :"+statistics.getTotalPartitionsProcessed());
            System.out.println("getDdlOperationPerformed :"+statistics.getDdlOperationPerformed());
//            System.out.println("------------------");
//            // Query 결과 Schema 만 가져올 수 있음
//            System.out.println(statistics.getSchema());
//            FieldList fl = statistics.getSchema().getFields();
//            for (Field field : fl){
//                System.out.println("-------------------");
//                System.out.println(field.getName());
//                System.out.println(field.getType().name());
//            }
//            // Query 결과에 대한 총 용량
//            System.out.println("getEstimatedBytesProcessed BytesProcess: " + statistics.getEstimatedBytesProcessed());
//            System.out.println("getTotalBytesBilled BytesProcess: " + statistics.getTotalBytesBilled());
//            System.out.println("getTotalBytesProcessed BytesProcess: " + statistics.getTotalBytesProcessed());
//            System.out.println("getNumDmlAffectedRows BytesProcess: " + statistics.getNumDmlAffectedRows());

//            queryJob = queryJob.waitFor();
//
//            if (queryJob == null) {
//                throw new RuntimeException("Job no longer exists");
//            } else if (queryJob.getStatus().getError() != null) {
//                throw new RuntimeException(queryJob.getStatus().getError().toString());
//            }

            TableResult results = bigQuery.query(QueryJobConfiguration.of(qry));
            System.out.println(results);

            long bigquery_after_time = System.currentTimeMillis();
            long bigquery_diff_time = (bigquery_after_time - bigquery_before_time) / 1000;
            System.out.println("#### bigquery End Time : " + bigquery_after_time + "####");
            System.out.println("#### bigquery Diff Time : " + bigquery_diff_time + "####");

            long download_before_time = System.currentTimeMillis();
            System.out.println("#### download Start Time : " + download_before_time + "####");
            GoogleCredentials googleCredentials = GCPConfig.accessToken();
            Storage storage = StorageOptions.newBuilder().setCredentials(googleCredentials).build().getService();

            Blob blob = storage.get(BlobId.of("spark-samples-data", storagePath));
            System.out.println(blob.getSize());
            blob.downloadTo(Paths.get(filePath));
            storage.delete(BlobId.of("spark-samples-data", storagePath));

            long download_after_time = System.currentTimeMillis();
            long download_diff_time = (download_after_time - download_before_time) / 1000;
            System.out.println("#### bigquery End Time : " + download_after_time + "####");
            System.out.println("#### bigquery Diff Time : " + download_diff_time + "####");


            long spark_before_time = System.currentTimeMillis();
            System.out.println("#### spark Start Time : " + spark_before_time + "####");
            // json 형태 데이터 -> spark dataset 변형
            Dataset<Row> df =  sparkSession.read().format("json")
                    .option("multiline", false)
                    .load(sparkPath);
            df.show();
            System.out.println("count : " + df.count());

            // dataset -> view 생성
            df.createOrReplaceTempView(key);
            long spark_after_time = System.currentTimeMillis();
            long spark_diff_time = (spark_after_time - spark_before_time) / 1000;
            System.out.println("#### spark End Time : " + spark_after_time + "####");
            System.out.println("#### spark Diff Time : " + spark_diff_time + "####");
            df.unpersist();
        } catch (Exception e){
            System.out.println(e.getMessage());
            System.out.println(Arrays.toString(e.getStackTrace()));
        }

        return "index";
    }
}
