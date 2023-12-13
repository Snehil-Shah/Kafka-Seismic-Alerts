package kafka.consumer;

import java.util.Properties;

public class ConsumerConfig
{
    private Properties config = new Properties();
    public ConsumerConfig(String client){
        config.put("bootstrap.servers","localhost:9092");
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("group.id",client);
    }
    public Properties getProperties(){
        return config;
    }
}
