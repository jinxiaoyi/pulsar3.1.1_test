package com.example.pulsar_test.config;

import org.apache.pulsar.client.api.*;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Component
public class PulsarConfig {

    private final Map<String, Producer<String>> producerMap = new ConcurrentHashMap<>();


    @EventListener(ApplicationReadyEvent.class)
    public void init() throws PulsarClientException {
        //定义一个flag防止多次加载
        PulsarClient pulsarClient = pulsarClient();
        String topic1 = "persistent://blackhole/test/topic1";
        String topic2 = "persistent://blackhole/test/topic2";
        Producer<String> topic1Producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic1).create();
        Producer<String> topic2Producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic2).create();

        producerMap.put("topic1", topic1Producer);
        producerMap.put("topic2", topic2Producer);

        Consumer<String> consumer1 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic1).subscriptionType(SubscriptionType.Key_Shared)
                .subscriptionName("my-subscription1").messageListener((consumer, msg) -> {
                    System.out.println("consumer1 start :" + msg.getValue());
                    try {
                        Thread.sleep(30000L);
                        consumer.acknowledge(msg);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).ackTimeout(30, TimeUnit.MINUTES)
                .subscribe();
        Consumer<String> consumer2 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic2).subscriptionType(SubscriptionType.Key_Shared)
                .subscriptionName("my-subscription2").messageListener((consumer, msg) -> {
                    System.out.println("consumer2 start :" + msg.getValue());
                    try {
                        Thread.sleep(30000L);
                        consumer.acknowledge(msg);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).ackTimeout(30, TimeUnit.MINUTES)
                .subscribe();


    }

    PulsarClient pulsarClient() throws PulsarClientException {
        String serviceUrl = "pulsar://ip:6650";
        ClientBuilder clientBuilder = PulsarClient.builder()
                .serviceUrl(serviceUrl).ioThreads(3) //用于处理到brokers的连接的线程数
                .listenerThreads(20);
        return clientBuilder.build();
    }

    @Bean
    Map<String, Producer<String>> producerMap() {
        return producerMap;
    }

}
