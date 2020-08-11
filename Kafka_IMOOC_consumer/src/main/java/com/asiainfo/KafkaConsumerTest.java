package com.asiainfo;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * kafka五个客户端之三——consumer client
 *
 */
public class KafkaConsumerTest {
	public static void main(String[] args) {
//		autoCommit();
		manualCommit();
//		manualCommitMutiThread();
//		consumeOnePartition();
//		consumeOnePartitionMultiThread(3);
//		consumeThreadPool();
//		consumeOffset();
//		controlPause();
	}

	/**
	 * 自动提交
	 */
	private static void autoCommit() {
		// 构建kafka消费者客户端
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "127.0.0.1:9092,127.0.0.1:9093");// 要链接的kafka地址
		// 注意：同一个partition的消息只能被处在同一个group下的某一个consumer消费，一个consumer可以消费多个partition的消息，即一对多关系，但是反过来一个partition只对应一个consumer。所以在一个group里，consumer的数量如果多余partition的数量，则多出来的consumer不会消费任何数据处于空闲状态。
		properties.setProperty("group.id", "test_group");// consumer的group，若干个consumer是互相独立的，它们可以组成一个组且拥有一个同样的组id。
		properties.setProperty("enable.auto.commit", "true");// 自动提交offset
		properties.setProperty("auto.commit.interval.ms", "1");// 每隔n毫秒提交一次，前提是enable.auto.commit设置为true才生效
		properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");// key反序列化的类
		properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");// value反序列化的类
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);// 泛型代表消息key和value的类型
		String topicName = "topicName2";
		kafkaConsumer.subscribe(Arrays.asList(topicName));
		ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(10000));// 客户端从kafka拉取数据，参数是每隔多长时间拉取一次。和客户端发送原理一样，获取也是批量获取，所以返回的是一个集合，每一个ConsumerRecord代表获取到的一个消息
		boolean flag = true;
		for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
			try {
				System.out.println("\t" + "consumerRecord = " + consumerRecord);
			} catch (Exception e) {
				flag = false;
				e.printStackTrace();
			}
		}
	}

	private static void manualCommit() {
		// 构建kafka消费者客户端
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");// 要链接的kafka地址
		// 注意：同一个partition的消息只能被处在同一个group下的某一个consumer消费，一个consumer可以消费多个partition的消息，即一对多关系，但是反过来一个partition只对应一个consumer。所以在一个group里，consumer的数量如果多余partition的数量，则多出来的consumer不会消费任何数据处于空闲状态。
		properties.setProperty("group.id", "test_group");// consumer的group，若干个consumer是互相独立的，它们可以组成一个组且拥有一个同样的组id。
//        properties.setProperty("enable.auto.commit", "true");// 自动提交offset
		properties.setProperty("enable.auto.commit", "false");// 手动提交offset，实际生产上手动提交较为常用，因为如果设置为自动提交那么每隔n毫秒就会强制提交一次offset，提交之后消费过的数据就不能再消费了，如果已消费的数据没有处理成功（比如保存数据库失败），该消息是要被再次获取的，所以要等到所有消息全部处理成功之后再手动提交一次
		properties.setProperty("auto.commit.interval.ms", "1000");// 每隔n毫秒提交一次，前提是enable.auto.commit设置为true才生效
		properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");// key反序列化的类
		properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");// value反序列化的类
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);// 泛型代表消息key和value的类型
		String topicName = "topicName2";
		kafkaConsumer.subscribe(Arrays.asList(topicName));
		ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(10000));// 客户端从kafka拉取数据，参数是每隔多长时间拉取一次。和客户端发送原理一样，获取也是批量获取，所以返回的是一个集合，每一个ConsumerRecord代表获取到的一个消息
		boolean flag = true;
		System.out.println("consumerRecords.size = " + consumerRecords.count());
		for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
			try {
				System.out.println("\t" + "consumerRecord = " + consumerRecord);
			} catch (Exception e) {
				flag = false;
				e.printStackTrace();
			}
		}

		if (flag) {
			// 手动提交
			System.out.println("全部处理成功，提交offset");
			kafkaConsumer.commitAsync();
		}
	}

	/**
	 * 一个consumer取出所有partition的数据，然后每个partition的数据分给不同的线程处理
	 */
	private static void manualCommitMutiThread() {
		// 构建kafka消费者客户端
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");// 要链接的kafka地址
		// 注意：同一个partition的消息只能被处在同一个group下的某一个consumer消费，一个consumer可以消费多个partition的消息，即一对多关系，但是反过来一个partition只对应一个consumer。所以在一个group里，consumer的数量如果多余partition的数量，则多出来的consumer不会消费任何数据处于空闲状态。
		properties.setProperty("group.id", "test_group");// consumer的group，若干个consumer是互相独立的，它们可以组成一个组且拥有一个同样的组id。
//        properties.setProperty("enable.auto.commit", "true");// 自动提交offset
		properties.setProperty("enable.auto.commit", "false");// 手动提交offset，实际生产上手动提交较为常用，因为如果设置为自动提交那么每隔n毫秒就会强制提交一次offset，提交之后消费过的数据就不能再消费了，如果已消费的数据没有处理成功（比如保存数据库失败），该消息是要被再次获取的，所以要等到所有消息全部处理成功之后再手动提交一次
		properties.setProperty("auto.commit.interval.ms", "1000");// 每隔n毫秒提交一次，前提是enable.auto.commit设置为true才生效
		properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");// key反序列化的类
		properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");// value反序列化的类
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);// 泛型代表消息key和value的类型
		String topicName = "topicName1";
		kafkaConsumer.subscribe(Arrays.asList(topicName));
		final ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(10000));// 客户端从kafka拉取数据，参数是每隔多长时间拉取一次。和客户端发送原理一样，获取也是批量获取，所以返回的是一个集合，每一个ConsumerRecord代表获取到的一个消息
		Set<TopicPartition> partitions = consumerRecords.partitions();
		int count = 0;
		for (TopicPartition topicPartition : partitions) {
			count++;
			new Thread(new Runnable() {
				@Override
				public void run() {
					List<ConsumerRecord<String, String>> recordList = consumerRecords.records(topicPartition);
					System.out.println(Thread.currentThread().getName() + "获取的partition为：" + topicPartition + "，数据量："
							+ recordList.size());
					boolean flag = true;
					for (ConsumerRecord<String, String> consumerRecord : recordList) {
						try {
							System.out.println("\t" + "consumerRecord = " + consumerRecord);
						} catch (Exception e) {
							flag = false;
							e.printStackTrace();
						}
					}

					if (flag) {
						// 手动提交
						System.out.println("全部处理成功，提交offset");
						long lastOffset = recordList.get(recordList.size() - 1).offset();
						Map<TopicPartition, OffsetAndMetadata> map = new HashMap<TopicPartition, OffsetAndMetadata>();
						map.put(topicPartition, new OffsetAndMetadata(lastOffset));
						kafkaConsumer.commitSync(map);
					}
				}
			}, "consumer_thread_" + count).start();
		}
	}

	/**
	 * consumer只从topic的其中一个partition获取数据，此方法和manualCommitMutiThread方法的区别在于前者是一个线程创建一个consumer，
	 * 此consumer只消费一个partition的数据，后者是一个consumer消费掉所有partition的数据，然后对取出来的数据根据partition的不同分配给不同的线程还处理
	 */
	private static void consumeOnePartitionMultiThread(int partitionCount) {
		for (int i = 0; i < partitionCount; i++) {
			new ConsumerThread("mythread-" + i, i).start();// 每个consumer消费某一个partition的数据
		}
	}

	static class ConsumerThread extends Thread {
		private KafkaConsumer<String, String> kafkaConsumer = null;// kafka的provider是线程安全的，consumer不是线程安全的，需要自己解决consumer的线程安全问题
		private int partitionNum;

		public ConsumerThread(String threadName, int partitionNum) {
			super.setName(threadName);
			this.partitionNum = partitionNum;

			Properties properties = new Properties();
			properties.setProperty("bootstrap.servers", "localhost:9092");
			properties.setProperty("group.id", "test_group");
			properties.setProperty("enable.auto.commit", "false");
			properties.setProperty("auto.commit.interval.ms", "1000");
			properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			kafkaConsumer = new KafkaConsumer<String, String>(properties);// 没启动一个线程创建一个新的kafkaConsumer，即kafkaConsumer不是线程共用的
			kafkaConsumer.assign(Arrays.<TopicPartition>asList(new TopicPartition("topicName1", partitionNum)));
		}

		@Override
		public void run() {
			ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(10000));
			boolean flag = true;
			System.out.println(Thread.currentThread().getName() + "消费partition-" + partitionNum
					+ ",consumerRecords.size = " + consumerRecords.count());
			for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
				try {
					System.out.println("\t" + "consumerRecord = " + consumerRecord);
				} catch (Exception e) {
					flag = false;
					e.printStackTrace();
				}
			}

			if (flag) {
				// 手动提交
				System.out.println("全部处理成功，提交offset");
				kafkaConsumer.commitAsync();
			}
		}
	}

	/**
	 * 该方法和manualCommitMutiThread的相同点都是一个consumer消费所有partition的数据，
	 * 不同点在于本方法是一个线程只负责处理partition的一条数据，而manualCommitMutiThread方法里面没有线程负责处理某个partition的所有数据
	 */
	public static void consumeThreadPool() {
		// 构建kafka消费者客户端
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");// 要链接的kafka地址
		// 注意：同一个partition的消息只能被处在同一个group下的某一个consumer消费，一个consumer可以消费多个partition的消息，即一对多关系，但是反过来一个partition只对应一个consumer。所以在一个group里，consumer的数量如果多余partition的数量，则多出来的consumer不会消费任何数据处于空闲状态。
		properties.setProperty("group.id", "test_group");// consumer的group，若干个consumer是互相独立的，它们可以组成一个组且拥有一个同样的组id。
//    	        properties.setProperty("enable.auto.commit", "true");// 自动提交offset
		properties.setProperty("enable.auto.commit", "false");// 手动提交offset，实际生产上手动提交较为常用，因为如果设置为自动提交那么每隔n毫秒就会强制提交一次offset，提交之后消费过的数据就不能再消费了，如果已消费的数据没有处理成功（比如保存数据库失败），该消息是要被再次获取的，所以要等到所有消息全部处理成功之后再手动提交一次
		properties.setProperty("auto.commit.interval.ms", "1000");// 每隔n毫秒提交一次，前提是enable.auto.commit设置为true才生效
		properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");// key反序列化的类
		properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");// value反序列化的类
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);// 泛型代表消息key和value的类型
		String topicName = "topicName1";
		kafkaConsumer.subscribe(Arrays.asList(topicName));
		ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(10000));// 客户端从kafka拉取数据，参数是每隔多长时间拉取一次。和客户端发送原理一样，获取也是批量获取，所以返回的是一个集合，每一个ConsumerRecord代表获取到的一个消息
		boolean flag = true;
		System.out.println("consumerRecords.size = " + consumerRecords.count());

		ExecutorService threadPool = Executors.newFixedThreadPool(5);
		for (final ConsumerRecord<String, String> consumerRecord : consumerRecords) {
			// 每个消息都交给一个线程来处理
			threadPool.execute(new Runnable() {
				@Override
				public void run() {
					System.out.println("\t" + Thread.currentThread().getName() + ",consumerRecord = " + consumerRecord);
				}
			});
		}

		if (flag) {
			// 手动提交
			System.out.println("全部处理成功，提交offset");
			kafkaConsumer.commitAsync();
			threadPool.shutdown();
		}
	}

	/**
	 * 指定offset消费数据
	 */
	public static void consumeOffset() {
		// 构建kafka消费者客户端
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");// 要链接的kafka地址
		// 注意：同一个partition的消息只能被处在同一个group下的某一个consumer消费，一个consumer可以消费多个partition的消息，即一对多关系，但是反过来一个partition只对应一个consumer。所以在一个group里，consumer的数量如果多余partition的数量，则多出来的consumer不会消费任何数据处于空闲状态。
		properties.setProperty("group.id", "test_group");// consumer的group，若干个consumer是互相独立的，它们可以组成一个组且拥有一个同样的组id。
//		    	        properties.setProperty("enable.auto.commit", "true");// 自动提交offset
		properties.setProperty("enable.auto.commit", "false");// 手动提交offset，实际生产上手动提交较为常用，因为如果设置为自动提交那么每隔n毫秒就会强制提交一次offset，提交之后消费过的数据就不能再消费了，如果已消费的数据没有处理成功（比如保存数据库失败），该消息是要被再次获取的，所以要等到所有消息全部处理成功之后再手动提交一次
		properties.setProperty("auto.commit.interval.ms", "1000");// 每隔n毫秒提交一次，前提是enable.auto.commit设置为true才生效
		properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");// key反序列化的类
		properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");// value反序列化的类
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);// 泛型代表消息key和value的类型
		String topicName = "topicName2";
//		kafkaConsumer.subscribe(Arrays.asList(topicName));
		TopicPartition topicPartition = new TopicPartition(topicName, 0);
		kafkaConsumer.assign(Arrays.<TopicPartition>asList(topicPartition));
		kafkaConsumer.seek(topicPartition, 375);// 从指定partition的375处开始消费（包含375）
		
		ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(10000));// 客户端从kafka拉取数据，参数是每隔多长时间拉取一次。和客户端发送原理一样，获取也是批量获取，所以返回的是一个集合，每一个ConsumerRecord代表获取到的一个消息
		boolean flag = true;
		System.out.println("consumerRecords.size = " + consumerRecords.count());

		for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
			System.out.println("\t" + Thread.currentThread().getName() + ",consumerRecord = " + consumerRecord);
		}

		if (flag) {
			// 手动提交
			System.out.println("全部处理成功，提交offset");
			kafkaConsumer.commitAsync();
//			threadPool.shutdown();
		}
	}
	
	/**
	 * 限流，即达到某个条件的时候就暂停consumer，达到另一个条件的时候唤醒consumer。
	 * 比如：每获取到一个消息就去令牌桶拿一个令牌，如果拿到了就继续业务处理，如果没拿到（说明并发量可能很大）就暂停consumer，等令牌桶有令牌的时候唤醒consumer
	 */
	public static void controlPause() {
		// 构建kafka消费者客户端
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");// 要链接的kafka地址
		// 注意：同一个partition的消息只能被处在同一个group下的某一个consumer消费，一个consumer可以消费多个partition的消息，即一对多关系，但是反过来一个partition只对应一个consumer。所以在一个group里，consumer的数量如果多余partition的数量，则多出来的consumer不会消费任何数据处于空闲状态。
		properties.setProperty("group.id", "test_group");// consumer的group，若干个consumer是互相独立的，它们可以组成一个组且拥有一个同样的组id。
//		    	        properties.setProperty("enable.auto.commit", "true");// 自动提交offset
		properties.setProperty("enable.auto.commit", "false");// 手动提交offset，实际生产上手动提交较为常用，因为如果设置为自动提交那么每隔n毫秒就会强制提交一次offset，提交之后消费过的数据就不能再消费了，如果已消费的数据没有处理成功（比如保存数据库失败），该消息是要被再次获取的，所以要等到所有消息全部处理成功之后再手动提交一次
		properties.setProperty("auto.commit.interval.ms", "1000");// 每隔n毫秒提交一次，前提是enable.auto.commit设置为true才生效
		properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");// key反序列化的类
		properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");// value反序列化的类
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);// 泛型代表消息key和value的类型
		String topicName = "topicName1";
//		kafkaConsumer.subscribe(Arrays.asList(topicName));
		TopicPartition topicPartition = new TopicPartition(topicName, 0);
		kafkaConsumer.assign(Arrays.<TopicPartition>asList(topicPartition));
		
		ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(10000));// 客户端从kafka拉取数据，参数是每隔多长时间拉取一次。和客户端发送原理一样，获取也是批量获取，所以返回的是一个集合，每一个ConsumerRecord代表获取到的一个消息
		boolean flag = true;
		System.out.println("consumerRecords.size = " + consumerRecords.count());
		
		boolean[] bs = new boolean[] {true,false,true,false,true};
		for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
			// 每获取到一个消息就去令牌桶拿一个令牌
			int i = (int) (Math.random() * 5);// 生成随机数模拟获取到令牌的情况，true为获取到，false为没有获取到	
			if(bs[i]) {// 获取到了令牌
				// 执行业务处理
				System.out.println("\t" + Thread.currentThread().getName() + ",consumerRecord = " + consumerRecord);
			} else {
				// 没获取到则暂停consumer
				System.out.println("暂停consumer");
				kafkaConsumer.pause(Arrays.asList(topicPartition));
			}
			
			int j = (int) (Math.random() * 5);
			if(bs[j]) {// 令牌中有了令牌
				System.out.println("唤醒consumer");
				kafkaConsumer.resume(Arrays.asList(topicPartition));
			}
		}
		
		if (flag) {
			// 手动提交
			System.out.println("全部处理成功，提交offset");
			kafkaConsumer.commitAsync();
		}
	}
}
