package com.test.spark.controller;

import com.test.spark.config.GCP.GCPConfig;
import com.test.spark.util.RedisUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/")
public class MainController {
    private JavaSparkContext sparkContext;
    private SparkSession sparkSession;
    private JavaStreamingContext streamingContext;
    private RedisUtil redisUtil;
    private GCPConfig GCPConfig;

    public MainController(JavaSparkContext sparkContext, SparkSession sparkSession, JavaStreamingContext streamingContext,
                          RedisUtil redisUtil, GCPConfig GCPConfig){
        this.sparkContext = sparkContext;
        this.sparkSession = sparkSession;
        this.streamingContext = streamingContext;
        this.redisUtil = redisUtil;
        this.GCPConfig = GCPConfig;
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