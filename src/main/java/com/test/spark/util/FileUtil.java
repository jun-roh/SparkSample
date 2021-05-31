package com.test.spark.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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
            File file = new File(path);
            if (!file.exists()){
                file.createNewFile();
            }

            BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));

            writer.write(object.toString());
            writer.flush();
            writer.close();
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
