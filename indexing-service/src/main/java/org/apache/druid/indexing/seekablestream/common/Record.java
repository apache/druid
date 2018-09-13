package org.apache.druid.indexing.seekablestream.common;

import java.util.List;

public class Record<T1, T2>
{
  public static final String END_OF_SHARD_MARKER = "EOS";

  private final String streamName;
  private final T1 partitionId;
  private final T2 sequenceNumber;
  private final List<byte[]> data;

  public Record(String streamName, T1 partitionId, T2 sequenceNumber, List<byte[]> data)
  {
    this.streamName = streamName;
    this.partitionId = partitionId;
    this.sequenceNumber = sequenceNumber;
    this.data = data;
  }

  public String getStreamName()
  {
    return streamName;
  }

  public T1 getPartitionId()
  {
    return partitionId;
  }

  public T2 getSequenceNumber()
  {
    return sequenceNumber;
  }

  public List<byte[]> getData()
  {
    return data;
  }

  public StreamPartition getStreamPartition()
  {
    return StreamPartition.of(streamName, partitionId);
  }
}
