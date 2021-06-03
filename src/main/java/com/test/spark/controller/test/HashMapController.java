package com.test.spark.controller.test;

import com.test.spark.config.GCP.GCPConfig;
import com.test.spark.data.SimpleData;
import com.test.spark.util.JsonUtil;
import com.test.spark.util.RedisUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.boot.configurationprocessor.json.JSONException;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Controller
@RequestMapping("/")
public class HashMapController {
    private JavaSparkContext sparkContext;
    private SparkSession sparkSession;
    private JavaStreamingContext streamingContext;
    private RedisUtil redisUtil;
    private GCPConfig GCPConfig;

    public HashMapController(JavaSparkContext sparkContext, SparkSession sparkSession, JavaStreamingContext streamingContext,
                             RedisUtil redisUtil, GCPConfig GCPConfig){
        this.sparkContext = sparkContext;
        this.sparkSession = sparkSession;
        this.streamingContext = streamingContext;
        this.redisUtil = redisUtil;
        this.GCPConfig = GCPConfig;
    }

    @GetMapping("/hashmap_spark")
    public String hashmapSpark(Model model) throws JSONException {
        // Hashmap 생성
        SimpleData simpleData = new SimpleData();
        List<HashMap<String, Object>> result = simpleData.setHashMapData();
        // List 로 row 마다 json string 으로 변경
        List<String> json_result = JsonUtil.convertListToJsonStringList(result);

        // Redis 저장
        redisUtil.setValue("json_data", json_result, 10, TimeUnit.SECONDS);

        // Redis 에서 가져옴
        List<String> get_json_string = redisUtil.getMapValue("json_data");

        // List<String> -> Dataset<String> format 변경
        Dataset<String> df = sparkSession.createDataset(get_json_string, Encoders.STRING());
        // Dataset<String> 형태로 Dataset<Row> 로 변경
        Dataset<Row> df1 = sparkSession.read().json(df);
        df1.show();

        return "index";
    }


}
