package org.apache.druid.indexing.kinesis.common;

public class StreamPartition
{
  private final String streamName;
  private final String partitionId;

  public StreamPartition(String streamName, String partitionId)
  {
    this.streamName = streamName;
    this.partitionId = partitionId;
  }

  public String getStreamName()
  {
    return streamName;
  }

  public String getPartitionId()
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

  public static StreamPartition of(String streamName, String partitionId)
  {
    return new StreamPartition(streamName, partitionId);
  }
}
