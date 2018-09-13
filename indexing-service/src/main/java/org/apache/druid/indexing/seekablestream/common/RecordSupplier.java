package org.apache.druid.indexing.seekablestream.common;

import java.io.Closeable;
import java.util.Set;
import java.util.concurrent.TimeoutException;

// TODO: need to integrate this with Kafka

/**
 * The RecordSupplier interface is a wrapper for the incoming seekable data stream
 * (i.e. Kafka consumer)
 */
public interface RecordSupplier<T1, T2> extends Closeable
{
  void assign(Set<StreamPartition<T1>> partitions);

  void seek(StreamPartition<T1> partition, T2 sequenceNumber);

  void seekAfter(StreamPartition<T1> partition, T2 sequenceNumber);

  void seekToEarliest(StreamPartition<T1> partition);

  void seekToLatest(StreamPartition<T1> partition);

  Record<T1, T2> poll(long timeout);

  T2 getLatestSequenceNumber(StreamPartition<T1> partition) throws TimeoutException;

  T2 getEarliestSequenceNumber(StreamPartition<T1> partition) throws TimeoutException;

  Set<T1> getPartitionIds(String streamName);

  @Override
  void close();
}
