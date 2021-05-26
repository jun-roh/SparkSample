package com.test.spark.controller;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.test.spark.util.JsonUtil;
import com.test.spark.util.RedisUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
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
import java.io.FileWriter;
import java.util.*;

@Controller
@RequestMapping("/")
public class MainController {
    private JavaSparkContext sparkContext;
    private SparkSession sparkSession;
    private JavaStreamingContext streamingContext;
    private RedisUtil redisUtil;

    public MainController(JavaSparkContext sparkContext, SparkSession sparkSession, JavaStreamingContext streamingContext,
                          RedisUtil redisUtil){
        this.sparkContext = sparkContext;
        this.sparkSession = sparkSession;
        this.streamingContext = streamingContext;
        this.redisUtil = redisUtil;
    }

//    @Autowired
//    private JavaSparkContext sparkContext;
//
//    @Autowired
//    private SparkSession sparkSession;
//
//    @Autowired
//    private JavaStreamingContext streamingContext;

    @GetMapping("/")
    public String index(Model model){
        // json to spark dataframe multiline
        Dataset<Row> df =  sparkSession.read().format("json")
                .option("multiline", true)
                .load("src/main/resources/data_line/json_data_line.json");
        df.show(5, 150);
        df.printSchema();

        // json to spark dataframe single line
        Dataset<Row> df_single =  sparkSession.read().format("json")
                .load("src/main/resources/data_line/json_data_single.json");
        df_single.show(5, 150);
        df_single = df_single.filter(df_single.col("owns").isNotNull());
        df_single.show(5, 150);


        return "index";
    }

    @GetMapping("/hashmap_spark")
    public String hashmapSpark(Model model) throws JSONException {
        // Hashmap 생성
        List<HashMap<String, Object>> result = setHashMapData();
        // List 로 row 마다 json string 으로 변경
        List<String> json_result = JsonUtil.convertListToJsonStringList(result);

//        JSONArray jsonArray = JsonUtil.convertListToJson(result);
//        String jsonString = jsonArray.toString();

        // Redis 저장
        redisUtil.setValue("json_data", json_result);

        // Redis 에서 가져옴
        List<String> get_json_string = redisUtil.getMapValue("json_data");

        // List<String> -> Dataset<String> format 변경
        Dataset<String> df = sparkSession.createDataset(get_json_string, Encoders.STRING());
        // Dataset<String> 형태로 Dataset<Row> 로 변경
        Dataset<Row> df1 = sparkSession.read().json(df);
        df1.show();

        return "index";
    }

    @GetMapping("/json_spark")
    public String jsonSpark(Model model) throws JSONException {
        List<HashMap<String, Object>> result = setHashMapData();
        JSONArray jsonArray = JsonUtil.convertListToJson(result);
        String jsonString = jsonArray.toString();

//        redisUtil.setValue("json_data", json_result);

        List<String> get_json_string = redisUtil.getMapValue("json_data");


//        Dataset<String> df = sparkSession.createDataset(get_json_string, Encoders.STRING());
//        Dataset<Row> df1 = sparkSession.read().json(df);
//        df1.show();

        return "index";
    }

    @GetMapping("/bigquery_spark")
    public String bigQuerySpark(Model model){
        BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
        System.out.println(bigQuery.toString());
        return "index";
    }

    public List<HashMap<String, Object>> setHashMapData(){
        List<HashMap<String, Object>> result = new ArrayList<>();
        HashMap<String, Object> map = new HashMap<>();
        map.put("idx", 1);
        map.put("national", "한국");
        map.put("value", 123);
        result.add(map);
        HashMap<String, Object> map1 = new HashMap<>();
        map1.put("idx", 2);
        map1.put("national", "미국");
        map1.put("value", 225);
        result.add(map1);
        HashMap<String, Object> map2 = new HashMap<>();
        map2.put("idx", 3);
        map2.put("national", "캐나다");
        map2.put("value", 345);
        result.add(map2);

        return result;
    }
}
