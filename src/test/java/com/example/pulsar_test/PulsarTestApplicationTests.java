package com.example.pulsar_test;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Map;
import java.util.UUID;

@SpringBootTest
class PulsarTestApplicationTests {


    @Autowired
    private Map<String, Producer<String>> producerMap;


    //When different topics, such as topic1 and topic2, both use Key_Shared, the consumer is blocked.
    @Test
    void test1() throws InterruptedException, PulsarClientException {
        System.out.println("test1 start");
        Producer<String> topic1 = producerMap.get("topic1");
        Producer<String> topic2 = producerMap.get("topic2");
        topic1.newMessage().key("key").value("value1").send();
        topic2.newMessage().key("key").value("value2").send();
        Thread.sleep(10000000L);
    }

}
