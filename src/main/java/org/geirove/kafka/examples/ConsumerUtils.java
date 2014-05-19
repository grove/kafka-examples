package org.geirove.kafka.examples;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

public class ConsumerUtils {

	public static String findNewLeader(Collection<BrokerReference> replicaBrokers, BrokerReference oldLeader, String topicId, int partitionId) throws Exception {
		for (int i = 0; i < 3; i++) {
			boolean goToSleep = false;
			PartitionMetadata metadata = findLeader(replicaBrokers, replicaBrokers, topicId, partitionId);
			if (metadata == null) {
				goToSleep = true;
			} else if (metadata.leader() == null) {
				goToSleep = true;
			} else if (oldLeader.getHostname().equalsIgnoreCase(metadata.leader().host()) && oldLeader.getPort() == metadata.leader().port() && i == 0) {
				// first time through if the leader hasn't changed give ZooKeeper a second to recover
				// second time, assume the broker did recover before failover, or it was a non-Broker issue
				//
				goToSleep = true;
			} else {
				return metadata.leader().host();
			}
			if (goToSleep) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ie) {
				}
			}
		}
		System.out.println("Unable to find new leader after Broker failure. Exiting");
		throw new Exception("Unable to find new leader after Broker failure. Exiting");
	}

	public static PartitionMetadata findLeader(Collection<BrokerReference> replicaBrokers, Collection<BrokerReference> seedBrokers, String topicId, int partitionId) {
		PartitionMetadata returnMetaData = null;
		loop:
			for (BrokerReference seed : seedBrokers) {
				SimpleConsumer consumer = null;
				try {
					consumer = new SimpleConsumer(seed.getHostname(), seed.getPort(), 100000, 64 * 1024, "leaderLookup");
					List<String> topics = Collections.singletonList(topicId);
					TopicMetadataRequest req = new TopicMetadataRequest(topics);
					kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

					List<TopicMetadata> metaData = resp.topicsMetadata();
					for (TopicMetadata item : metaData) {
						for (PartitionMetadata part : item.partitionsMetadata()) {
							if (part.partitionId() == partitionId) {
								returnMetaData = part;
								break loop;
							}
						}
					}
				} catch (Exception e) {
					System.out.println("Error communicating with Broker [" + seed + "] to find Leader for [" + topicId
							+ ", " + partitionId + "] Reason: " + e);
				} finally {
					if (consumer != null) consumer.close();
				}
			}
		if (returnMetaData != null) {
			replicaBrokers.clear();
			for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
				replicaBrokers.add(new BrokerReference(replica.host(), replica.port()));
			}
		}
		return returnMetaData;
	}

	public static final class BrokerReference {

		private String hostname;
		private int port;

		public BrokerReference(String hostname, int port) {
			this.hostname = hostname;
			this.port = port;
		}

		public String getHostname() {
			return hostname;
		}

		public void setHostname(String hostname) {
			this.hostname = hostname;
		}

		public int getPort() {
			return port;
		}

		public void setPort(int port) {
			this.port = port;
		}

		@Override
		public boolean equals(Object o) {
			if (o instanceof BrokerReference) {
				BrokerReference other = (BrokerReference)o;
				return other.hostname.equals(hostname) && other.port == port;
			}
			return false;
		}

		@Override
		public int hashCode() {
			return hostname.hashCode() + port;
		}

	}

}
