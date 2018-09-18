package org.apache.druid.indexing.kinesis.common;

import java.io.Closeable;
import java.util.Set;
import java.util.concurrent.TimeoutException;

public interface RecordSupplier extends Closeable
{
  void assign(Set<StreamPartition> partitions);

  void seek(StreamPartition partition, String sequenceNumber);

  void seekAfter(StreamPartition partition, String sequenceNumber);

  void seekToEarliest(StreamPartition partition);

  void seekToLatest(StreamPartition partition);

  Record poll(long timeout);

  String getLatestSequenceNumber(StreamPartition partition) throws TimeoutException;

  String getEarliestSequenceNumber(StreamPartition partition) throws TimeoutException;

  Set<String> getPartitionIds(String streamName);

  void close();
}
