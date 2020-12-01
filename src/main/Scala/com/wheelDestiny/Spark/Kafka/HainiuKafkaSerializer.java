package com.wheelDestiny.Spark.Kafka;

import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

public class HainiuKafkaSerializer implements Serializer<Object> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        if (data == null){
            return null;
        }else {
            byte[] bytes = null;
            try (
                    ByteArrayOutputStream bo = new ByteArrayOutputStream();
                    ObjectOutputStream oi = new ObjectOutputStream(bo);
                    ) {
                oi.writeObject(data);
                bytes = bo.toByteArray();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return bytes;
        }
    }

    @Override
    public void close() {
    }
}
