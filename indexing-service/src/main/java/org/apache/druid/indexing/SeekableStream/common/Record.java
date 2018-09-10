package org.apache.druid.indexing.SeekableStream.common;

import java.util.List;

public class Record<PartitionType, SequenceType>
{
  public static final String END_OF_SHARD_MARKER = "EOS";

  private final String streamName;
  private final PartitionType partitionId;
  private final SequenceType sequenceNumber;
  private final List<byte[]> data;

  public Record(String streamName, PartitionType partitionId, SequenceType sequenceNumber, List<byte[]> data)
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

  public PartitionType getPartitionId()
  {
    return partitionId;
  }

  public SequenceType getSequenceNumber()
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
