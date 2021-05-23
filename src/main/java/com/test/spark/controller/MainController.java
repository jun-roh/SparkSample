package com.test.spark.controller;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.io.File;
import java.util.Arrays;

@Controller
@RequestMapping("/")
public class MainController {
    @Autowired
    private JavaSparkContext sparkContext;

    @Autowired
    private SparkSession sparkSession;

    @Autowired
    private JavaStreamingContext streamingContext;

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
}
