package com.test.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import scala.Tuple2;

import java.io.File;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

@SpringBootApplication
public class SparkSampleApplication {
    public static void main(String[] args) {
        SpringApplication.run(SparkSampleApplication.class, args);
    }
}
