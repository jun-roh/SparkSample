package com.test.spark.controller;

import com.google.cloud.bigquery.*;
import com.test.spark.config.Bigquery.BigqueryConfig;
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
import java.io.File;
import java.io.FileWriter;
import java.util.*;
import java.util.stream.Collectors;

@Controller
@RequestMapping("/")
public class MainController {
    private JavaSparkContext sparkContext;
    private SparkSession sparkSession;
    private JavaStreamingContext streamingContext;
    private RedisUtil redisUtil;
    private BigqueryConfig bigqueryConfig;

    public MainController(JavaSparkContext sparkContext, SparkSession sparkSession, JavaStreamingContext streamingContext,
                          RedisUtil redisUtil, BigqueryConfig bigqueryConfig){
        this.sparkContext = sparkContext;
        this.sparkSession = sparkSession;
        this.streamingContext = streamingContext;
        this.redisUtil = redisUtil;
        this.bigqueryConfig = bigqueryConfig;
    }

    @GetMapping("/")
    public String index(Model model){
        // Sample
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
}