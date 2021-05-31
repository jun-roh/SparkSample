package com.test.spark.controller.test;

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
        String query = "select * from de_test.test_table";

        try {
            // redis key 존재하지 않으면 Bigquery 에서 데이터 가져와서 json 에 넣도록 함
            if (redisUtil.getValue(key) == null){
                BigQuery bigQuery = bigqueryConfig.setBigquery();

                // Bigquery Query 처리
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

                // Bigquery result -> json
                List<String> fieldList = result.getSchema().getFields().stream()
                        .map(Field::getName)
                        .collect(Collectors.toList());
                JSONArray jsonArray = JsonUtil.convertTableResultToJson(result, fieldList);

                // file 생성
                fileUtil.setFile(path, jsonArray);
                // redis 생성 (생명주기 부여)
                redisUtil.setValue(key, path, 100, TimeUnit.SECONDS);
            }

            // json 형태 데이터 -> spark dataset 변형
            Dataset<Row> df =  sparkSession.read().format("json")
                    .option("multiline", true)
                    .load(path);
            df.show();

            // dataset -> view 생성
            df.createOrReplaceTempView(key);

            // view 에 원하는 쿼리를 통해서 spark 연산
            List<String> df_string = sparkSession.sql("select idx, name, value from "+key+" where idx >= 5")
                    .toJSON().collectAsList();

            System.out.println(df_string);
        } catch (Exception e){
            System.out.println("-----");
            System.out.println(e.getMessage());
            System.out.println(Arrays.toString(e.getStackTrace()));
        }

        return "index";
    }

    @GetMapping("/bigquery_storage")
    public String bigQueryStorage(Model model){
        String query = "select * from de_test.test_table";
        String sourceUri = "gs://spark-samples-data/sample/test_*.json";
        String dataFormat = "JSON";
        try {
            BigQuery bigQuery = bigqueryConfig.setBigquery();
            String qry =
                    String.format(
                            "EXPORT DATA OPTIONS(uri='%s', format='%s', overwrite=true) "
                                    + "AS " + query,
                            sourceUri, dataFormat);

            TableResult results = bigQuery.query(QueryJobConfiguration.of(qry));

            results
                    .iterateAll()
                    .forEach(row -> row.forEach(val -> System.out.printf("%s,", val.toString())));
        } catch (Exception e){
            System.out.println(e.getMessage());
            System.out.println(Arrays.toString(e.getStackTrace()));
        }

        return "index";
    }
}
