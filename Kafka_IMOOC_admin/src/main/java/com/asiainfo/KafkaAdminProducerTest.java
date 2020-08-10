package com.asiainfo;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * kafka五个客户端之二——producer client
 *
 * @author zhangzhiwang
 * @date Aug 1, 2020 3:44:16 PM
 */
public class KafkaAdminProducerTest {
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		// 构建producer客户端对象
		Properties properties = new Properties();// 实际项目中properties的值可以写到配置文件里面
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "z1:9092");// 要连接的kafka地址
		properties.put(ProducerConfig.ACKS_CONFIG, "all");
		properties.put(ProducerConfig.RETRIES_CONFIG, "0");
		properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
		properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
		properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "123321");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		
//		properties.put("security.protocol","SSL");
//		properties.put("sl.endpoint.identification.algorithm","");
//		properties.put("ssl.truststore.location","client.truststore.jks");
//		properties.put("ssl.truststore.password","zzw1234");
		
		Producer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		String topicName = "topicName1";
		for(int i =0; i < 100; i++) {
			// kafka发送的消息要包装在ProducerRecord中，一个ProducerRecord就是一条被发送到kafka的消息
			ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topicName, "key-" + i, "value-" + i);
			producer.send(producerRecord);// kafka是异步发送数据的
//			Future<RecordMetadata> result = producer.send(producerRecord);// send方法的返回值是个Future，Future在get的时候会阻塞当前线程，所以不使用send方法的返回值就是异步发送，使用了send方法返回值就是同步发送
//			RecordMetadata recordMetadata = result.get();// kafka是同步发送数据还是异步发送数据取决于用不用send方法的返回值
//			System.out.println("recordMetadata : " + recordMetadata);
			
//			producer.send(producerRecord, new Callback() {// 带回调的发送方法
//				@Override
//				public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
//					System.out.println("call back recordMetadata : " + recordMetadata);
//				}});
		}
		
		// producer用完之后要关闭
		producer.close();
	}
}
	