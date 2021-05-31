package com.test.spark.controller.test;

import com.test.spark.config.Bigquery.BigqueryConfig;
import com.test.spark.data.SimpleData;
import com.test.spark.util.JsonUtil;
import com.test.spark.util.RedisUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.boot.configurationprocessor.json.JSONArray;
import org.springframework.boot.configurationprocessor.json.JSONException;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Controller
@RequestMapping("/")
public class JsonController {
    private JavaSparkContext sparkContext;
    private SparkSession sparkSession;
    private JavaStreamingContext streamingContext;
    private RedisUtil redisUtil;
    private BigqueryConfig bigqueryConfig;

    public JsonController(JavaSparkContext sparkContext, SparkSession sparkSession, JavaStreamingContext streamingContext,
                              RedisUtil redisUtil, BigqueryConfig bigqueryConfig){
        this.sparkContext = sparkContext;
        this.sparkSession = sparkSession;
        this.streamingContext = streamingContext;
        this.redisUtil = redisUtil;
        this.bigqueryConfig = bigqueryConfig;
    }

    @GetMapping("/json_spark")
    public String jsonSpark(Model model) throws JSONException {
        SimpleData simpleData = new SimpleData();
        List<HashMap<String, Object>> result = simpleData.setHashMapData();
        JSONArray jsonArray = JsonUtil.convertListToJson(result);
        String jsonString = jsonArray.toString();
        String path = "src/main/resources/temp/json_data.json";
        try {
            // json array file 쓰기
            File file = new File(path);
            if (!file.exists()){
                // Redis 저장
                redisUtil.setValue("json_data", "jsonString", 10, TimeUnit.SECONDS);
                file.createNewFile();
            }

            BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));

            writer.write(jsonArray.toString());
            writer.flush();
            writer.close();

            // json file 을 dataset spark 이용
            Dataset<Row> df =  sparkSession.read().format("json")
                    .option("multiline", true)
                    .load(path);
            df.show();

        } catch (Exception e){
            e.printStackTrace();
        }

        return "index";
    }
}
