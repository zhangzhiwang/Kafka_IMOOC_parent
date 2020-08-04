package com.asiainfo;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * kafka五个客户端之三——consumer client
 *
 */
public class KafkaConsumerTest {
	public static void main(String[] args) {
		// 构建kafka消费者客户端
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");// 要链接的kafka地址
		// 注意：同一个partition的消息只能被处在同一个group下的某一个consumer消费，一个consumer可以消费多个partition的消息，即一对多关系，但是反过来一个partition只对应一个consumer。所以在一个group里，consumer的数量如果多余partition的数量，则多出来的consumer不会消费任何数据处于空闲状态。
        properties.setProperty("group.id", "test_group");// consumer的group，若干个consumer是互相独立的，它们可以组成一个组且拥有一个同样的组id。
        properties.setProperty("enable.auto.commit", "true");// 自动提交offset
        properties.setProperty("auto.commit.interval.ms", "1000");// 每隔n毫秒提交一次
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");// key反序列化的类
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");// value反序列化的类
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);// 泛型代表消息key和value的类型
		String topicName = "topicName1";
		kafkaConsumer.subscribe(Arrays.asList(topicName));
		ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(10000));// 客户端从kafka拉取数据，参数是每隔多长时间拉取一次。和客户端发送原理一样，获取也是批量获取，所以返回的是一个集合，每一个ConsumerRecord代表获取到的一个消息
		for(ConsumerRecord<String, String> consumerRecord : consumerRecords) {
			System.out.println("\t" + "consumerRecord = " + consumerRecord);
		}
	}
}
