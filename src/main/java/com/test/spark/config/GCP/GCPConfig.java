package com.test.spark.config.GCP;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.FileInputStream;
import java.nio.file.Paths;

@Configuration
public class GCPConfig {
    @Bean
    public synchronized BigQuery setBigquery(){
        BigQuery bigQuery = null;
        String json_path = System.getProperty("user.dir") + "/src/main/resources/bigquery/test_credential.json";
        String project = "lgpublic";
        try {
            FileInputStream fileInputStream = new FileInputStream(json_path);
            GoogleCredentials googleCredentials = ServiceAccountCredentials.fromStream(fileInputStream);
            bigQuery = BigQueryOptions.newBuilder().setCredentials(googleCredentials).setProjectId(project).build().getService();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return bigQuery;
    }

    @Bean
    public synchronized GoogleCredentials accessToken(){
        GoogleCredentials googleCredentials = null;
        String json_path = System.getProperty("user.dir") + "/src/main/resources/bigquery/test_credential.json";
        try {
            FileInputStream fileInputStream = new FileInputStream(json_path);
            googleCredentials = ServiceAccountCredentials.fromStream(fileInputStream);
        } catch (Exception e){
            e.printStackTrace();
        }

        return googleCredentials;
    }
}
