/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.firehose.kafka;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.ParseException;
import com.apple.jobjc.Utils.Threads;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;

import io.druid.data.input.ByteBufferInputRowParser;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.firehose.kafka.KafkaSimpleConsumer.BytesMessageWithOffset;

public class KafkaEightSimpleConsumerFirehoseFactory implements
        FirehoseFactory<ByteBufferInputRowParser> {
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

	@JsonProperty
	private final ByteBufferInputRowParser parser;

	private final Map<Integer, KafkaSimpleConsumer> simpleConsumerMap = new HashMap<Integer, KafkaSimpleConsumer>();

	private final static int fileRetention = 5;

	private long lastCommitTime = 0;

	private final Map<Integer, Long> lastOffsetPartitions = new HashMap<Integer, Long>();

	private static final String PARTITION_SEPERATOR = " ";
	private static final String PARTITION_OFFSET_SEPERATOR = ",";
	private static final String FILE_NAME_SEPERATOR = "_";

	private MessageDigest md;

	private FixedFileArrayList<File> offsetFileList = new FixedFileArrayList<File>(
	        fileRetention);

	private List<Thread> consumerThreadList = new ArrayList<Thread>();
	
	private  InputRow nextMsg;
	
	private boolean stop;

	@JsonCreator
	public KafkaEightSimpleConsumerFirehoseFactory(
	        @JsonProperty("brokerList") String brokerList,
	        @JsonProperty("partitionIdList") String partitionIdList,
	        @JsonProperty("clientId") String clientId,
	        @JsonProperty("feed") String feed,
	        @JsonProperty("parser") ByteBufferInputRowParser parser,
	        @JsonProperty("offsetPosition") String offsetPosition,
	        @JsonProperty("queueBufferLength") int queueBufferLength) {
		this.brokerList = brokerList;
		this.partitionIdList = partitionIdList;
		this.clientId = clientId;
		this.feed = feed;
		this.parser = (parser == null) ? null : parser;
		this.queueBufferLength = queueBufferLength;
		this.offsetPosition = offsetPosition;
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			log.info("no md5 algorithm");
		}
	}

	private String getMd5String(String content){
		md.update(content.getBytes(), 0, content.length());
		return new BigInteger(1, md.digest()).toString(16);
	}
	private void loadOffsetFromDisk() {

		File offsetPosistion = new File(offsetPosition);
		if (!offsetPosistion.exists()) {
			offsetPosistion.mkdirs();
		}
		File[] listFiles = offsetPosistion.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String fileName) {
				return true;
			}
		});
		for (File file : listFiles)
			Arrays.sort(listFiles, new Comparator<File>() {
				public int compare(File f1, File f2) {
					return Long.valueOf(f1.lastModified()).compareTo(
					        f2.lastModified());
				}
			});

		for (File file : listFiles) {
			try {
				offsetFileList.add(file);
				String fileName = file.getName();
				String fileTimeStamp = fileName.split(FILE_NAME_SEPERATOR)[0];
				String md5String = fileName.split(FILE_NAME_SEPERATOR)[1];

				byte[] fileContents = null;
				try {
					fileContents = Files
					        .readAllBytes(Paths.get(file.getPath()));
				} catch (IOException e) {
					log.error("can't read file when load offset from disk", e);
				}
				if (!md5String.equals(getMd5String(new String(fileContents)))) {
					log.info("offset file  [%s] file name and content mismatch,  [%s] file content md5: [%s]" ,fileTimeStamp,new String(fileContents),getMd5String(new String(fileContents)));
					file.delete();
					continue;
				}
				if (file.lastModified() > lastCommitTime) {
					lastCommitTime = file.lastModified();
				}
				String fileContentString = new String(fileContents);
				String[] partitionsOffset = null;
				if (fileContentString.contains(PARTITION_SEPERATOR)) {
					partitionsOffset = fileContentString
					        .split(PARTITION_SEPERATOR);
				} else {
					partitionsOffset = new String[1];
					partitionsOffset[0] = fileContentString;
				}
				for (String partitionOffsetString : partitionsOffset) {
					if (!partitionOffsetString.trim().equals("")) {
						String[] partitionOffset = partitionOffsetString
						        .split(PARTITION_OFFSET_SEPERATOR);
						if (lastOffsetPartitions.containsKey(Integer
						        .parseInt(partitionOffset[0]))
						        && lastOffsetPartitions.get(Integer
						                .parseInt(partitionOffset[0])) >= Long
						                .parseLong(partitionOffset[1])) {
							continue;
						}
						lastOffsetPartitions.put(
						        Integer.parseInt(partitionOffset[0]),
						        Long.parseLong(partitionOffset[1]));
					}
				}
			} catch (Exception e) {
				file.delete();
			}
		}
		log.info("lastoffset commit time: " + lastCommitTime);
		log.info("offset map: " + lastOffsetPartitions);

	}

	@Override
	public Firehose connect(final ByteBufferInputRowParser firehoseParser)
	        throws IOException {
		Set<String> newDimExclus = Sets.union(firehoseParser.getParseSpec()
		        .getDimensionsSpec().getDimensionExclusions(),
		        Sets.newHashSet("feed"));
		final ByteBufferInputRowParser theParser = firehoseParser
		        .withParseSpec(firehoseParser.getParseSpec()
		                .withDimensionsSpec(
		                        firehoseParser.getParseSpec()
		                                .getDimensionsSpec()
		                                .withDimensionExclusions(newDimExclus)));
		final LinkedBlockingQueue<BytesMessageWithOffset> messageQueue = new LinkedBlockingQueue(
		        queueBufferLength);
		class partitionConsumerThread extends Thread {
			private int partitionId;
			private boolean stop = false;

			partitionConsumerThread(int partitionId) {
				this.partitionId = partitionId;
			}

			@Override
			public void run() {
				log.info("start running parition [%s] thread name : [%s]",partitionId, getName());
				try {
					Long offset = lastOffsetPartitions.get(partitionId);
					if (offset == null) {
						offset = 0L;
					}
					while (!isInterrupted()) {
						try {
							log.debug("start fetching " + partitionId
							        + " feed " + feed);
							Iterable<BytesMessageWithOffset> msgs = simpleConsumerMap
							        .get(partitionId).fetch(offset, 10000);
							int count = 0;
							for (BytesMessageWithOffset msgWithOffset : msgs) {
								offset = msgWithOffset.offset();
								messageQueue.put(msgWithOffset);
								count++;
							}
							log.debug("fetch [%s] msgs for partition [%s] in one time", count, partitionId);
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

		loadOffsetFromDisk();
		log.info("kicking off all consumer");
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
		log.info("all consumer started");
		return new Firehose() {
			@Override
			public boolean hasMore() {
				if(stop){
					return false;
				}
				BytesMessageWithOffset msg;
				try {
					msg = messageQueue.take();
				} catch (InterruptedException e) {
					log.info(" interrupted when taken from queue");
					nextMsg = null;
					return true;
				}
				final byte[] message = msg.message();

				if (message == null) {
					nextMsg = null;
					return true;
				}
				lastOffsetPartitions.put(msg.getPartition(), msg.offset());
				nextMsg = theParser.parse(ByteBuffer.wrap(message));
				return true;
			}

			@Override
			public InputRow nextRow() {
				return nextMsg;
			}

			@Override
			public Runnable commit() {
				final java.util.Date date = new java.util.Date();

				StringBuilder fileContent = new StringBuilder();
				for (int partition : lastOffsetPartitions.keySet()) {
					fileContent.append(partition
					        + PARTITION_OFFSET_SEPERATOR
					        + lastOffsetPartitions.get(partition)
					        + PARTITION_SEPERATOR);
				}
				final String fileName =getMd5String(fileContent.toString());
				final String fileContentStr = fileContent.toString();
				return new Runnable() {
					
					@Override
					public void run() {
						

						File file = new File(offsetPosition + File.separator
						        + date.getTime() + FILE_NAME_SEPERATOR
						        + fileName);
						try {
							// if file doesnt exists, then create it
							if (!file.exists()) {
								file.createNewFile();
							}
							FileWriter fw = new FileWriter(
							        file.getAbsoluteFile());
							BufferedWriter bw = new BufferedWriter(fw);
							bw.write(fileContentStr);
							bw.close();
							log.info("committing offsets");
							offsetFileList.add(file);
							lastCommitTime = file.lastModified();
						} catch (Exception e) {
							log.info(
							        "exception happens when creating offset file",
							        e);
						}
					}
				};
			}

			@Override
			public void close() throws IOException {
				log.info("stoping kafka 0.8 simple firehose");
				stop = true;
				for (Thread t : consumerThreadList) {
					try {
						t.interrupt();
						t.join(3000);
					} catch (InterruptedException e) {
						log.info("interupted when stoping ");
					}
				}
			}

			@Override
			public long getLastOffsetCommitTime() {
				return lastCommitTime;
			}
		};
	}

	@Override
	public InputRowParser getParser() {
		// TODO Auto-generated method stub
		return parser;
	}
	
}
