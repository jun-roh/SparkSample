package com.test.spark.controller.test;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import com.test.spark.config.Bigquery.BigqueryConfig;
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
    private BigqueryConfig bigqueryConfig;
    private FileUtil fileUtil;

    public BigQueryController(JavaSparkContext sparkContext, SparkSession sparkSession, JavaStreamingContext streamingContext,
                          RedisUtil redisUtil, FileUtil fileUtil, BigqueryConfig bigqueryConfig){
        this.sparkContext = sparkContext;
        this.sparkSession = sparkSession;
        this.streamingContext = streamingContext;
        this.redisUtil = redisUtil;
        this.fileUtil = fileUtil;
        this.bigqueryConfig = bigqueryConfig;
    }

    @GetMapping("/bigquery_spark")
    public String bigQuerySpark(Model model){
        String key = "json_file";
        String path = "src/main/resources/temp/"+key+".json";
        String query = TestQuery.query_2;

        try {
            // redis key 존재하지 않으면 Bigquery 에서 데이터 가져와서 json 에 넣도록 함
            if (redisUtil.getValue(key) == null){
                long bigquery_before_time = System.currentTimeMillis();
                System.out.println("#### bigquery Start Time : " + bigquery_before_time + "####");

                // Bigquery Query 처리
                BigQuery bigQuery = bigqueryConfig.setBigquery();
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
                BigQuery bigQuery = bigqueryConfig.setBigquery();
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

        String query = TestQuery.query_2;
        String sourceUri = "gs://spark-samples-data/sample/test.json";
        String dataFormat = "JSON";
        try {
            long bigquery_before_time = System.currentTimeMillis();
            System.out.println("#### bigquery Start Time : " + bigquery_before_time + "####");
            BigQuery bigQuery = bigqueryConfig.setBigquery();
            String qry =
                    String.format(
                            "EXPORT DATA OPTIONS(uri='%s', format='%s', overwrite=true) "
                                    + "AS " + query,
                            sourceUri, dataFormat);

            TableResult results = bigQuery.query(QueryJobConfiguration.of(qry));
            System.out.println(results.getSchema().getFields().toString());
            long bigquery_after_time = System.currentTimeMillis();
            long bigquery_diff_time = (bigquery_after_time - bigquery_before_time) / 1000;
            System.out.println("#### bigquery End Time : " + bigquery_after_time + "####");
            System.out.println("#### bigquery Diff Time : " + bigquery_diff_time + "####");

//            // Sample
//            // json to spark dataframe multiline
//            Dataset<Row> df =  sparkSession.read().format("json")
//                    .load("src/main/resources/data_line/json_data_line.json");
//            df.show(5, 150);
//            df.printSchema();

        } catch (Exception e){
            System.out.println(e.getMessage());
            System.out.println(Arrays.toString(e.getStackTrace()));
        }

        return "index";
    }
}
