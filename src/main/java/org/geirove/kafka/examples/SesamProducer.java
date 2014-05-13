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


import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import au.com.bytecode.opencsv.CSVReader;

public class SesamProducer {

	/**
	 * Create topic with this command: 
	 * 
	 *   kafka-topics.sh --create --topic ams --partitions 3 --zookeeper 192.168.50.5:2181 --replication-factor 2
	 *   
	 * List topics:
	 * 
	 *   kafka-topics.sh --zookeeper 192.168.50.5:2181 --list
	 *   
	 * Tail topic:
	 * 
	 *   kafka-console-consumer.sh --zookeeper 192.168.50.5:2181 --topic ams --from-beginning
	 * 
	 */
	public static void main(String[] args) throws Exception {
		String brokerList = "192.168.50.20:9092";
		String topic = "ams";
		String inputFile = "/Users/geir.gronmo/Downloads/DateValue.txt";
		if (args.length != 3) {
			System.out.println("Usage: org.geirove.kafka.examples.SesamProducer <brokerList> <topic> <inputFile>");
			//			System.exit(1);
		} else {
			brokerList = args[0];
			topic = args[1];
			inputFile = args[2];
		}
		System.out.println("brokerList: " + brokerList);
		System.out.println("topic: " + brokerList);
		System.out.println("inputFile: " + brokerList);

		run(brokerList, topic, inputFile);
	}

	public static void run(String brokerList, String topic, String inputFile) throws Exception {
		Properties props = new Properties();
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list", brokerList);

		Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(props));
		try {
			FileInputStream istream = new FileInputStream(inputFile);
			CSVReader csvReader = new CSVReader(new InputStreamReader(istream, "UTF-16"));
			try {
				String[] row = null;
				while((row = csvReader.readNext()) != null) {
					if (row.length > 0) {
						System.out.println(Arrays.asList(row));
						String key = row[0];
						String value = join(row, "\t");
						producer.send(new KeyedMessage<String, String>(topic, key, value));
					}
				}
			} finally {
				csvReader.close();
			}
		} finally {
			producer.close();
		}
	}

	private static String join(String[] row, String separator) {
		StringBuilder sb = new StringBuilder();
		int i=0;
		for (String r : row) {
			if (i > 0) {
				sb.append(separator);
			}
			sb.append(r);
			i++;
		}
		return sb.toString();
	}
}
