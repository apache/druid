package org.apache.druid.indexing.kafka;

import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.common.Record;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

public class KafkaRecordSupplier implements RecordSupplier<Integer, Long>
{
  private static final Random RANDOM = new Random();

  private final KafkaConsumer<byte[], byte[]> consumer;
  private final KafkaSupervisorIOConfig ioConfig;
  private boolean closed;


  public KafkaRecordSupplier(
      KafkaSupervisorIOConfig ioConfig
  )
  {
    this.ioConfig = ioConfig;
    this.consumer = getKafkaConsumer();
    this.closed = false;
  }

  @Override
  public void assign(Set<StreamPartition<Integer>> streamPartitions)
  {
    consumer.assign(streamPartitions
                        .stream()
                        .map(x -> new TopicPartition(x.getStreamName(), x.getPartitionId()))
                        .collect(Collectors.toSet()));
  }

  @Override
  public void seek(StreamPartition<Integer> partition, Long sequenceNumber)
  {
    consumer.seek(new TopicPartition(partition.getStreamName(), partition.getPartitionId()), sequenceNumber);
  }

  @Override
  public void seekAfter(StreamPartition<Integer> partition, Long sequenceNumber)
  {
    seek(partition, sequenceNumber + 1);
  }

  @Override
  public void seekToEarliest(Set<StreamPartition<Integer>> partitions)
  {
    consumer.seekToBeginning(partitions
                                 .stream()
                                 .map(e -> new TopicPartition(e.getStreamName(), e.getPartitionId()))
                                 .collect(Collectors.toList()));
  }

  @Override
  public void seekToLatest(Set<StreamPartition<Integer>> partitions)
  {
    consumer.seekToEnd(partitions
                           .stream()
                           .map(e -> new TopicPartition(e.getStreamName(), e.getPartitionId()))
                           .collect(Collectors.toList()));
  }

  @Override
  public Set<StreamPartition<Integer>> getAssignment()
  {
    Set<TopicPartition> topicPartitions = consumer.assignment();
    return topicPartitions
        .stream()
        .map((TopicPartition e) -> new StreamPartition<>(e.topic(), e.partition()))
        .collect(Collectors.toSet());
  }

  @Override
  public Record<Integer, Long> poll(long timeout)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Long getLatestSequenceNumber(StreamPartition<Integer> partition)
  {
    seekToLatest(Collections.singleton(partition));
    return consumer.position(new TopicPartition(partition.getStreamName(), partition.getPartitionId()));
  }

  @Override
  public Long getEarliestSequenceNumber(StreamPartition<Integer> partition)
  {
    seekToEarliest(Collections.singleton(partition));
    return consumer.position(new TopicPartition(partition.getStreamName(), partition.getPartitionId()));
  }

  @Override
  public Long position(StreamPartition<Integer> partition)
  {
    return consumer.position(new TopicPartition(partition.getStreamName(), partition.getPartitionId()));
  }

  @Override
  public Set<Integer> getPartitionIds(String streamName)
  {
    final Map<String, List<PartitionInfo>> topics = consumer.listTopics();
    if (topics == null || !topics.containsKey(streamName)) {
      throw new ISE("Could not retrieve partitions for topic [%s]", streamName);
    }
    return topics.get(streamName).stream().map(PartitionInfo::partition).collect(Collectors.toSet());
  }

  @Override
  public void close()
  {
    if (closed) {
      return;
    }
    closed = true;
    consumer.close();
  }

  private KafkaConsumer<byte[], byte[]> getKafkaConsumer()
  {
    final Properties props = new Properties();

    props.setProperty("metadata.max.age.ms", "10000");
    props.setProperty("group.id", StringUtils.format("kafka-supervisor-%s", getRandomId()));

    props.putAll(ioConfig.getConsumerProperties());

    props.setProperty("enable.auto.commit", "false");

    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      return new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }
  }

  private static String getRandomId()
  {
    final StringBuilder suffix = new StringBuilder(8);
    for (int i = 0; i < Integer.BYTES * 2; ++i) {
      suffix.append((char) ('a' + ((RANDOM.nextInt() >>> (i * 4)) & 0x0F)));
    }
    return suffix.toString();
  }
}
