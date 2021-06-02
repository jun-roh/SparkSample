package com.test.spark.util;

import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.boot.configurationprocessor.json.JSONException;
import org.springframework.boot.configurationprocessor.json.JSONObject;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.List;

@Component
public class FileUtil {
    public void setFile(String path, Object object){
        try {
            // json array file 쓰기
            long file_before_time = System.currentTimeMillis();
            System.out.println("#### file Start Time : " + file_before_time + "####");
            File file = new File(path);
            if (!file.exists()){
                file.createNewFile();
            }

            BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));

            writer.write(object.toString());
            writer.flush();
            writer.close();
            long file_after_time = System.currentTimeMillis();
            long file_diff_time = (file_after_time - file_before_time) / 1000;
            System.out.println("#### file End Time : " + file_after_time + "####");
            System.out.println("#### file Diff Time : " + file_diff_time + "####");
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public void setFileFromTableResultJson(String path, TableResult tableResult){
        try {
            // json array file 쓰기
            long file_before_time = System.currentTimeMillis();
            System.out.println("#### file Start Time : " + file_before_time + "####");
            File file = new File(path);
            if (!file.exists()){
                file.createNewFile();
            }
            BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));
            for (FieldValueList row: tableResult.iterateAll()){
                writer.write(row.get(0).getStringValue());
                writer.newLine();
                writer.flush();
            }
            writer.close();
            long file_after_time = System.currentTimeMillis();
            long file_diff_time = (file_after_time - file_before_time) / 1000;
            System.out.println("#### file End Time : " + file_after_time + "####");
            System.out.println("#### file Diff Time : " + file_diff_time + "####");
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public void delFile(String path){
        try {
            // json array file 쓰기
            File file = new File(path);
            if (file.exists()){
                if (file.delete())
                    System.out.println("## " + path + " :  file deleted !");
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
