package com.asiainfo;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

/**
 * 自定义Partitioner，可以指定哪些消息发送到哪个指定partition
 * 
 *
 * @author zhangzhiwang
 * @date Aug 11, 2020 10:51:10 PM
 */
public class MyPartitioner implements Partitioner {

	@Override
	public void configure(Map<String, ?> configs) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		String keyStr = (String) key;
		int i = Integer.parseInt(keyStr);
		return i % 2;// 返回值表示partition的索引值，即代表发到哪个partition
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
	
}
