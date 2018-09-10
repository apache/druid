package org.apache.druid.indexing.SeekableStream.common;

public class StreamPartition<PartitionType>
{
  private final String streamName;
  private final PartitionType partitionId;

  public StreamPartition(String streamName, PartitionType partitionId)
  {
    this.streamName = streamName;
    this.partitionId = partitionId;
  }

  public String getStreamName()
  {
    return streamName;
  }

  public PartitionType getPartitionId()
  {
    return partitionId;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StreamPartition that = (StreamPartition) o;

    if (streamName != null ? !streamName.equals(that.streamName) : that.streamName != null) {
      return false;
    }
    return !(partitionId != null ? !partitionId.equals(that.partitionId) : that.partitionId != null);
  }

  @Override
  public int hashCode()
  {
    int result = streamName != null ? streamName.hashCode() : 0;
    result = 31 * result + (partitionId != null ? partitionId.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "StreamPartition{" +
           "streamName='" + streamName + '\'' +
           ", partitionId='" + partitionId + '\'' +
           '}';
  }

  public static <PartitionType> StreamPartition of(String streamName, PartitionType partitionId)
  {
    return new StreamPartition<>(streamName, partitionId);
  }
}

