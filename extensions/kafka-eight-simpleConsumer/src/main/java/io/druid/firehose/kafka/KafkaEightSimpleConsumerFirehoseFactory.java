/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.firehose.kafka;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.json.JSONObject;

import com.metamx.common.logger.Logger;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;

import io.druid.data.input.ByteBufferInputRowParser;
import io.druid.data.input.Committer;
import io.druid.data.input.FirehoseFactoryV2;
import io.druid.data.input.FirehoseV2;
import io.druid.data.input.InputRow;
import io.druid.firehose.kafka.KafkaSimpleConsumer.BytesMessageWithOffset;

public class KafkaEightSimpleConsumerFirehoseFactory implements
        FirehoseFactoryV2<ByteBufferInputRowParser> {
	private static final Logger log = new Logger(
	        KafkaEightSimpleConsumerFirehoseFactory.class);

	@JsonProperty
	private final String brokerList;

	@JsonProperty
	private final String partitionIdList;

	@JsonProperty
	private final String clientId;

	@JsonProperty
	private final String feed;

	@JsonProperty
	private final String offsetPosition;

	@JsonProperty
	private final int queueBufferLength;

	private final Map<Integer, KafkaSimpleConsumer> simpleConsumerMap = new HashMap<Integer, KafkaSimpleConsumer>();
	private final Map<Integer, Long> lastOffsetPartitions = new HashMap<Integer, Long>();

	private List<Thread> consumerThreadList = new ArrayList<Thread>();
	
	private  InputRow currMsg;
	private BytesMessageWithOffset msg;
	private boolean stop;

	@JsonCreator
	public KafkaEightSimpleConsumerFirehoseFactory(
	        @JsonProperty("brokerList") String brokerList,
	        @JsonProperty("partitionIdList") String partitionIdList,
	        @JsonProperty("clientId") String clientId,
	        @JsonProperty("feed") String feed,
	        @JsonProperty("offsetPosition") String offsetPosition,
	        @JsonProperty("queueBufferLength") int queueBufferLength) {
		this.brokerList = brokerList;
		this.partitionIdList = partitionIdList;
		this.clientId = clientId;
		this.feed = feed;
		this.queueBufferLength = queueBufferLength;
		this.offsetPosition = offsetPosition;
	}
	private void loadOffsetFromPreviousMetaData(Object lastCommit) {
		if (lastCommit == null) {
			return;
		}
		try {
			JSONObject lastCommitObject = new JSONObject(lastCommit.toString());
			@SuppressWarnings("unchecked")
      Iterator<String> keysItr = lastCommitObject.keys();
			while(keysItr.hasNext()) {
				String key = keysItr.next();
	         int partitionId = Integer.parseInt(key);
	         Long offset = lastCommitObject.getLong(key);
	         log.debug("Recover last commit information partitionId [%s], offset [%s]", partitionId, offset);
	         if (lastOffsetPartitions.containsKey(partitionId)) {
			        if(lastOffsetPartitions.get(partitionId) < offset) {
			        	lastOffsetPartitions.put(partitionId, offset);
			        }
					} else {
						lastOffsetPartitions.put(partitionId, offset);
					}
	    
	     }
		} catch (Exception e) {
			log.error("Fail to load offset from previous meta data [%s]", lastCommit);
		}

		log.info("Loaded offset map: " + lastOffsetPartitions);

	}

	@Override
	public FirehoseV2 connect(final ByteBufferInputRowParser firehoseParser, Object lastCommit) throws IOException 
	{
		Set<String> newDimExclus = Sets.union(
				firehoseParser.getParseSpec().getDimensionsSpec().getDimensionExclusions(),
		        Sets.newHashSet("feed")
				);
		final ByteBufferInputRowParser theParser = firehoseParser.withParseSpec(
				firehoseParser.getParseSpec()
		                .withDimensionsSpec(
		                        firehoseParser.getParseSpec()
		                                .getDimensionsSpec()
		                                .withDimensionExclusions(
		                                		newDimExclus
		                                		)
		                		)
				);
		final LinkedBlockingQueue<BytesMessageWithOffset> messageQueue = new LinkedBlockingQueue<BytesMessageWithOffset>(queueBufferLength);
		class partitionConsumerThread extends Thread {
			private int partitionId;
			partitionConsumerThread(int partitionId) {
				this.partitionId = partitionId;
			}

			@Override
			public void run() {
				log.info("Start running parition [%s] thread name : [%s]",partitionId, getName());
				try {
					Long offset = lastOffsetPartitions.get(partitionId);
					if (offset == null) {
						offset = 0L;
					}
					while (!isInterrupted()) {
						try {
							Iterable<BytesMessageWithOffset> msgs = simpleConsumerMap
							        .get(partitionId).fetch(offset, 10000);
							int count = 0;
							for (BytesMessageWithOffset msgWithOffset : msgs) {
								offset = msgWithOffset.offset();
								messageQueue.put(msgWithOffset);
								count++;
							}
							log.debug("fetch [%s] msgs for partition [%s] in one time ", count, partitionId);
						} catch (InterruptedException e) {
							log.error("Intrerupted when fecthing data");
							return;
						}
					}
				} finally {
					simpleConsumerMap.get(partitionId).stop();
				}
			}

		}
		;

		//loadOffsetFromDisk();
		loadOffsetFromPreviousMetaData(lastCommit);
		log.info("Kicking off all consumer");
		@SuppressWarnings("unused")
    final Iterator<BytesMessageWithOffset> iter = messageQueue.iterator();

		for (String partitionStr : Arrays.asList(partitionIdList.split(","))) {
			int partition = Integer.parseInt(partitionStr);
			final KafkaSimpleConsumer kafkaSimpleConsumer = new KafkaSimpleConsumer(
			        feed, partition, clientId, Arrays.asList(brokerList
			                .split(",")));
			simpleConsumerMap.put(partition, kafkaSimpleConsumer);
			Thread t = new partitionConsumerThread(partition);
			consumerThreadList.add(t);
			t.start();
		}
		log.info("All consumer started");
		return new FirehoseV2() {
			@Override
			public void start() throws Exception {
				try {
					msg = messageQueue.take();
				} catch (InterruptedException e) {
					log.info("Interrupted when taken from queue");
					currMsg = null;
				}
			}
			@Override
			public boolean advance() { 
				if(stop){
					return false;
				}
				try {
					msg = messageQueue.take();
				} catch (InterruptedException e) {
					log.info("Interrupted when taken from queue");
					currMsg = null;
				}
				
				return true;
			}

			@Override
			public InputRow currRow() {
				lastOffsetPartitions.put(msg.getPartition(), msg.offset());
				final byte[] message = msg.message();

				if (message == null) {
					currMsg = null;
				} else {
					currMsg = theParser.parse(ByteBuffer.wrap(message));
				}
				return currMsg;
			}

			@Override
			public Committer makeCommitter() {

				Object thisCommit = new JSONObject(lastOffsetPartitions);

				class MyCommitter implements Committer {
					private Object metaData;
					public void setMetaData(Object metaData) {
						this.metaData = metaData;
					}
					@Override
					public Object getMetadata() {
						return metaData;
					}
					@Override
					public void run() {
					}
				};
				MyCommitter committer = new MyCommitter();
				committer.setMetaData(thisCommit);
				return committer;
			}

			@Override
			public void close() throws IOException {
				log.info("Stoping kafka 0.8 simple firehose");
				stop = true;
				for (Thread t : consumerThreadList) {
					try {
						t.interrupt();
						t.join(3000);
					} catch (InterruptedException e) {
						log.info("Interupted when stoping ");
					}
				}
			}
		};
	}
	
}
