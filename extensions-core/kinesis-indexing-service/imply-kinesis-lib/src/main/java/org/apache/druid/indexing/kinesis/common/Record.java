package org.apache.druid.indexing.kinesis.common;

import java.util.List;

public class Record
{
  public static final String END_OF_SHARD_MARKER = "EOS";

  private final String streamName;
  private final String partitionId;
  private final String sequenceNumber;
  private final List<byte[]> data;

  public Record(String streamName, String partitionId, String sequenceNumber, List<byte[]> data)
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

  public String getPartitionId()
  {
    return partitionId;
  }

  public String getSequenceNumber()
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
