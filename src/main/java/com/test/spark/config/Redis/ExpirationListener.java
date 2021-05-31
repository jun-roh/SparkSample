package com.test.spark.config.Redis;

import com.test.spark.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.KeyExpirationEventMessageListener;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.stereotype.Component;

import javax.inject.Qualifier;
import java.io.File;

@Component
public class ExpirationListener extends KeyExpirationEventMessageListener {
    /**
     * Creates new {@link MessageListener} for {@code __keyevent@*__:expired} messages.
     *
     * @param listenerContainer must not be {@literal null}.
     */
    public ExpirationListener(RedisMessageListenerContainer listenerContainer) {
        super(listenerContainer);
    }

    /** * * @param message redis key * @param pattern __keyevent@*__:expired */
    @Override public void onMessage(Message message, byte[] pattern) {
        System.out.println("########## onMessage pattern " + new String(pattern) + " | " + message.toString());
        String key = message.toString();
        String path = "src/main/resources/temp/"+key+".json";
        FileUtil fileUtil = new FileUtil();
        fileUtil.delFile(path);
    }
}
