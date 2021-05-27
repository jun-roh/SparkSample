package com.test.spark.config.Bigquery;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.FileInputStream;

@Configuration
public class BigqueryConfig {
    @Bean
    public synchronized BigQuery setBigquery(){
        BigQuery bigQuery = null;
        String json_path = System.getProperty("user.dir") + "/src/main/resources/bigquery/test_credential.json";
        String project = "lgpublic";
        try {
            System.out.println(1);
            FileInputStream fileInputStream = new FileInputStream(json_path);
            System.out.println(2);
            GoogleCredentials googleCredentials = ServiceAccountCredentials.fromStream(fileInputStream);
            System.out.println(3);
            bigQuery = BigQueryOptions.newBuilder().setCredentials(googleCredentials).setProjectId(project).build().getService();
            System.out.println(4);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return bigQuery;
    }
}
