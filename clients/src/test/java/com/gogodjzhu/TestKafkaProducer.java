package com.gogodjzhu;

import kafka.serializer.StringEncoder;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 测试向本地server发送数据
 *
 * @author DJ.Zhu
 */
public class TestKafkaProducer {

    private static Logger LOG = LoggerFactory.getLogger(TestKafkaProducer.class);

    private static DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HHmmss");

    private KafkaProducer<String, Object> producer;

    @Before
    public void before() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        properties.put("serializer.class", StringEncoder.class.getName());
        this.producer = new KafkaProducer<>(properties);
    }

    @Test
    public void testSendSimple() throws InterruptedException, ExecutionException {
        producer.send(new ProducerRecord<String, Object>("test", dateFormat.format(new Date()))).get();
        producer.send(new ProducerRecord<String, Object>("test", dateFormat.format(new Date()))).get();
        producer.send(new ProducerRecord<String, Object>("test", dateFormat.format(new Date()))).get();
    }

}
