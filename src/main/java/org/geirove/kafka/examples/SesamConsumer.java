/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.geirove.kafka.examples;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

import org.geirove.kafka.examples.ConsumerUtils.BrokerReference;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.Broker;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

public class SesamConsumer {

	private static final class Result {
		private final int numRead;
		private final long nextOffset;
		Result(int numRead, long nextOffset) {
			this.numRead = numRead;
			this.nextOffset = nextOffset;
		}
		public int getNumRead() {
			return numRead;
		}
		public long getNextOffset() {
			return nextOffset;
		}
	}
	
	private static Result printMessages(ByteBufferMessageSet messageSet) throws UnsupportedEncodingException {
		int numRead = 0;
		long nextOffset = Integer.MIN_VALUE;
		for (MessageAndOffset messageAndOffset: messageSet) {
			long offset = messageAndOffset.offset();
			nextOffset = messageAndOffset.nextOffset();
			Message message = messageAndOffset.message();
			if (message.hasKey()) {				
				System.out.println("K: " + toString(message.key()) + " O: " + offset + " " + nextOffset);
			}
//			System.out.println("V: " + toString(message.payload()));
			numRead++;
		}
		return new Result(numRead, nextOffset);
	}

	private static String toString(ByteBuffer bb) {
		byte[] bytes = new byte[bb.limit()];
		bb.get(bytes);
		try {
			return new String(bytes, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static void main(String[] args) throws Exception {
		String brokerList = "192.168.50.20:9092,192.168.50.21:9092,192.168.50.22:9092";
		String topicId = "ams2";
		int partitionId = 0;
		long offset = 0L;

		if (args.length < 3) {
			System.out.println("Usage: org.geirove.kafka.examples.SesamConsumer <brokerList> <topic> <partition> <offset>");
			System.exit(1);
		} else {
			brokerList = args[0];
			topicId = args[1];
			partitionId = Integer.parseInt(args[2]);
			if (args.length > 3) {
				offset = Integer.parseInt(args[3]);
			}
		}
		Collection<BrokerReference> seedBrokers = parseBrokerList(brokerList);

		String clientId = "sesam-consumer";
		
		int totalRead = 0;
		int batches = 0;
		
		while (true) {
			batches++;
			Result r = readMessages(seedBrokers, clientId, topicId, partitionId, offset);
			if (r.getNumRead() > 0) {
				totalRead += r.getNumRead();
				offset = r.getNextOffset();
			} else {
				break;
			}
		}
		System.out.println("Batches: " + batches);
		System.out.println("Total messages: " + totalRead);
	}

	private static Collection<BrokerReference> parseBrokerList(String brokerList) {
		Collection<BrokerReference> result = new ArrayList<BrokerReference>();
		for (String ref : brokerList.split(",")) {
			String[] hostport = ref.split(":");
			String host = hostport[0];
			int port = Integer.parseInt(hostport[1]);
			result.add(new BrokerReference(host, port));
		}
//		result.add(new BrokerReference("192.168.50.20", 9092));
//		result.add(new BrokerReference("192.168.50.21", 9092));
//		result.add(new BrokerReference("192.168.50.22", 9092));
		return result;
	}

	private static Result readMessages(Collection<BrokerReference> seedBrokers,
			String clientId, String topicId,
			int partitionId, long offset) throws UnsupportedEncodingException {
		
        PartitionMetadata metadata = ConsumerUtils.findLeader(seedBrokers, seedBrokers, topicId, partitionId);
        if (metadata == null) {
            throw new RuntimeException("Can't find metadata for Topic and Partition. Exiting");
        }
        if (metadata.leader() == null) {
        	throw new RuntimeException("Can't find Leader for Topic and Partition. Exiting");
        }
        Broker leader = metadata.leader();
		String leaderHost = leader.host();
		int leaderPort = leader.port();
		System.out.println("Connecting to " + leaderHost + ":" + leaderPort);
		
		SimpleConsumer simpleConsumer = new SimpleConsumer(leaderHost, leaderPort,
				100000, 64*1024, clientId);
		try {

			FetchRequest req = new FetchRequestBuilder()
			.clientId(clientId)
			.addFetch(topicId, partitionId, offset, 100000)
			.build();
			
			FetchResponse fetchResponse = simpleConsumer.fetch(req);
			if (fetchResponse.hasError()) {
				System.out.println("Error code: " + fetchResponse.errorCode(topicId, partitionId));
			}
			ByteBufferMessageSet messageSet = (ByteBufferMessageSet) fetchResponse.messageSet(topicId, partitionId);
			
			System.out.println("Bytes: " + messageSet.validBytes());
			Result r = printMessages(messageSet);
			System.out.println("Messages: " + r.getNumRead() + " " + r.getNextOffset());
			return r;
		} finally {
			simpleConsumer.close();
		}
	}
	
}
