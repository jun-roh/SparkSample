package com.test.spark.config.Spark;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Properties;

@Getter
@Setter
@ConfigurationProperties("spark")
public class SparkProperties {

    private String appname;

    private String master;

    private Properties props = new Properties();

    private StreamingProperties streaming = new StreamingProperties();

    @Getter
    @Setter
    @ConfigurationProperties("streaming")
    public static class StreamingProperties {
        private Integer duration;
    }

}
