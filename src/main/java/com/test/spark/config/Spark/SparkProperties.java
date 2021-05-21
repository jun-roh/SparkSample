package com.test.spark.config.Spark;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Properties;

@Getter
@Setter
@ConfigurationProperties("spark")
public class SparkProperties {
    private String appName;

    private String master;

    private Properties props = new Properties();

    private StreamingProperties streaming = new StreamingProperties();

    @ConfigurationProperties("streaming")
    public static class StreamingProperties {

        private Integer duration;

        public Integer getDuration() {
            return duration;
        }

        public void setDuration(Integer duration) {
            this.duration = duration;
        }

    }
}
