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

	private Map<Integer, KafkaSimpleConsumer> simpleConsumerMap = new HashMap<Integer, KafkaSimpleConsumer>();

	private final static int fileRetention = 5;

	private long lastCommitTime = 0;

	private Map<Integer, Long> lastOffsetPartitions = new HashMap<Integer, Long>();

	private static final String PARTITION_SEPERATOR = " ";
	private static final String PARTITION_OFFSET_SEPERATOR = ",";
	private static final String FILE_NAME_SEPERATOR = "_";

	private MessageDigest md;

	private FixedFileArrayList<File> offsetFileList = new FixedFileArrayList<File>(
	        fileRetention);

	private List<Thread> consumerThreadList = new ArrayList<Thread>();

	@JsonCreator
	public KafkaEightSimpleConsumerFirehoseFactory(
	        @JsonProperty("brokerList") String brokerList,
	        @JsonProperty("partitionIdList") String partitionIdList,
	        @JsonProperty("clientId") String clientId,
	        @JsonProperty("feed") String feed,
	        @JsonProperty("parser") ByteBufferInputRowParser parser,
	        @JsonProperty("offsetPosition") String offsetPosition,
	        @JsonProperty("queueBufferLength") int queueBufferLength) {
		log.info("brokerList:" + brokerList);
		this.brokerList = brokerList;
		log.info("partitionIdList:" + partitionIdList);
		this.partitionIdList = partitionIdList;
		log.info("clientId:" + clientId);
		this.clientId = clientId;
		log.info("feed:" + feed);
		this.feed = feed;
		this.parser = (parser == null) ? null : parser;
		log.info("queueBufferLength:" + queueBufferLength);
		this.queueBufferLength = queueBufferLength;
		log.info("offsetPosition:" + offsetPosition);
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
					log.info("offset file " + fileTimeStamp
					        + "file name and content mismatch, "
					        + new String(fileContents) + "file content md5:"
					        + getMd5String(new String(fileContents)));
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
						log.info(Arrays.toString(partitionOffset));
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
				log.info("start running parition " + partitionId
				        + " thread name :" + getName());
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
							log.debug("fetch " + count + " msgs for partition "
							        + partitionId + " in one time");
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
				return true;
			}

			@Override
			public InputRow nextRow() {
				BytesMessageWithOffset msg;
				try {
					msg = messageQueue.take();
				} catch (InterruptedException e) {
					log.info(" interrupted when taken fromm queue");
					return null;
				}
				final byte[] message = msg.message();

				if (message == null) {
					return null;
				}
				lastOffsetPartitions.put(msg.getPartition(), msg.offset());
				return theParser.parse(ByteBuffer.wrap(message));
			}

			@Override
			public Runnable commit() {
				return new Runnable() {
					@Override
					public void run() {
						java.util.Date date = new java.util.Date();

						StringBuilder fileContent = new StringBuilder();
						for (int partition : lastOffsetPartitions.keySet()) {
							fileContent.append(partition
							        + PARTITION_OFFSET_SEPERATOR
							        + lastOffsetPartitions.get(partition)
							        + PARTITION_SEPERATOR);
						}
						String fileName =getMd5String(fileContent.toString());

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
							bw.write(fileContent.toString());
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
				log.info("stoping partition print trace ----------");
				for (StackTraceElement ste : Thread.currentThread()
				        .getStackTrace()) {
					log.info(ste.toString());
				}
				log.info("stoping partition ----------");
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
			
			public Runnable commitOffset(final Map<Integer, Long> offset){
				return new Runnable() {
					@Override
					public void run() {
						java.util.Date date = new java.util.Date();

						StringBuilder fileContent = new StringBuilder();
						for (int partition : offset.keySet()) {
							fileContent.append(partition
							        + PARTITION_OFFSET_SEPERATOR
							        + offset.get(partition)
							        + PARTITION_SEPERATOR);
						}
						String fileName = getMd5String(fileContent.toString());

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
							bw.write(fileContent.toString());
							bw.close();

							log.info("committing offsets");
							offsetFileList.add(file);
							lastCommitTime = file.lastModified();
						} catch (Exception e) {
							log.info(
							        "exception happens when creating offset file"+
							        e.getMessage());
							log.info("stackTrace" + Arrays.toString(e.getStackTrace()));
							        
						}
					}
				};
			}
			
			public Map<Integer, Long> getCurrentOffset(){
				return new HashMap<Integer, Long>(lastOffsetPartitions);
			}
		};
	}

	@Override
	public InputRowParser getParser() {
		// TODO Auto-generated method stub
		return parser;
	}
	
}
