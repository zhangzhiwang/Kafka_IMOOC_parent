package com.asiainfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.config.ConfigResource;

/**
 * kafka五个客户端之一——admin client
 *
 * @author zhangzhiwang
 * @date Aug 1, 2020 1:07:41 PM
 */
public class KafkaAdminClientTest {
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		// 获取kafkaadmin client客户端
		Properties props = new Properties();
		props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");// 要连接的kafka地址。kafka只有集群的概念没有单点的概念，即使是单点在kafka里面也叫集群
		AdminClient adminClient = AdminClient.create(props);

		// 创建topic
		String topicName = "topicName1";
		NewTopic newTopic = new NewTopic(topicName, // topic名称
				1, // 分区数量
				(short) 1);// 副本因子
		List<NewTopic> newTopicList = new ArrayList<>();
		newTopicList.add(newTopic);
		// 相当于命令行：bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic topicName
		CreateTopicsResult createTopicsResult = adminClient.createTopics(newTopicList);
		System.out.println("createTopicsResult : " + createTopicsResult);

		// 查看topic
//		ListTopicsResult listTopics = adminClient.listTopics();// 获取自定义的kafka topic，内置的topic不会返回
		ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
		listTopicsOptions.listInternal(true);// 查询内置的topic
		ListTopicsResult listTopics = adminClient.listTopics(listTopicsOptions);// 要想查询内置的topic就要调用该方法，效果相当于命令：bin/kafka-topics.sh --list --zookeeper localhost:2181
		Set<String> topicSet = listTopics.names().get();
		System.out.println("topicSet : " + topicSet);

		Collection<TopicListing> topicListings = listTopics.listings().get();
		System.out.println("topicListings : " + topicListings);

		Map<String, TopicListing> topicListingMap = listTopics.namesToListings().get();
		System.out.println("topicListingMap : " + topicListingMap);

		// 删除topic
//		List<String> delTopics = new ArrayList<String>();
//		delTopics.add("topicName");
//		DeleteTopicsResult deleteTopics = adminClient.deleteTopics(delTopics);
//		listTopics = adminClient.listTopics();// 删除完再查询一下看是否删除掉了
//		topicSet = listTopics.names().get();
//		System.out.println("topicSet : " + topicSet);

		// 查询topic的描述信息
//		DescribeTopicsResult describeTopics = adminClient.describeTopics(Arrays.asList(""));
		DescribeTopicsOptions describeTopicsOptions = new DescribeTopicsOptions();
		describeTopicsOptions.includeAuthorizedOperations(true);// 返回的topic信息是否包括有关授权的一些信息，默认为false
		DescribeTopicsResult describeTopics = adminClient.describeTopics(Arrays.asList(topicName), describeTopicsOptions);
		Map<String, TopicDescription> describeTopicMap = describeTopics.all().get();
		System.out.println("describeTopicMap : " + describeTopicMap);

		// 查询topic的配置信息
		ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, "topicName");
//		DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Arrays.asList(configResource));// 不带选项
		DescribeConfigsOptions describeConfigsOptions = new DescribeConfigsOptions();
		describeConfigsOptions.includeSynonyms(true);
		DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Arrays.asList(configResource), describeConfigsOptions);// 带选项
		Map<ConfigResource, Config> configResourceMap = describeConfigsResult.all().get();
		System.out.println("configResourceMap : " + configResourceMap);

		// 修改topic的配置信息
		Map<ConfigResource, Collection<AlterConfigOp>> map = new HashMap<ConfigResource, Collection<AlterConfigOp>>();
		ConfigResource cr = new ConfigResource(ConfigResource.Type.TOPIC, "topicName");// 设置要修改的topic名称
		AlterConfigOp alterConfigOp = new AlterConfigOp(new ConfigEntry("preallocate", "true"), OpType.SET);// 第一个参数是要修改的属性，第二个参数是操作的类型（SET为修改）

		map.put(cr, Arrays.asList(alterConfigOp));
		adminClient.incrementalAlterConfigs(map);

		// 修改完再查询以下topic的配置信息，看是否修改成功
		configResource = new ConfigResource(ConfigResource.Type.TOPIC, "topicName");
		describeConfigsResult = adminClient.describeConfigs(Arrays.asList(configResource));
		configResourceMap = describeConfigsResult.all().get();
		System.out.println("configResourceMap : " + configResourceMap);
		
		// 为topic增加partition
		NewPartitions newPartition = NewPartitions.increaseTo(3);// 从方法名increaseTo可以看出本方法是“增加到”而不是“增加”，增加到3个即增加后一共有3个partition
		Map<String, NewPartitions> newPartitionMap = new HashMap<String, NewPartitions>();
		newPartitionMap.put(topicName, newPartition);
		adminClient.createPartitions(newPartitionMap);
		
		Map<String, TopicDescription> describeTopicMap2 = describeTopics.all().get();
		System.out.println("describeTopicMap2 : " + describeTopicMap2);
	}
}
