package org.apache.druid.indexing.SeekableStream.common;

import java.io.Closeable;
import java.util.Set;
import java.util.concurrent.TimeoutException;

/**
 * The RecordSupplier interface is a wrapper for the incoming seekable data stream
 * (i.e. Kafka consumer)
 */
public interface RecordSupplier<PartitionType, SequenceType> extends Closeable
{
  void assign(Set<StreamPartition<PartitionType>> partitions);

  void seek(StreamPartition<PartitionType> partition, SequenceType sequenceNumber);

  void seekAfter(StreamPartition<PartitionType> partition, SequenceType sequenceNumber);

  void seekToEarliest(StreamPartition<PartitionType> partition);

  void seekToLatest(StreamPartition<PartitionType> partition);

  Record<PartitionType, SequenceType> poll(long timeout);

  SequenceType getLatestSequenceNumber(StreamPartition<PartitionType> partition) throws TimeoutException;

  SequenceType getEarliestSequenceNumber(StreamPartition<PartitionType> partition) throws TimeoutException;

  Set<PartitionType> getPartitionIds(String streamName);

  @Override
  void close();
}
