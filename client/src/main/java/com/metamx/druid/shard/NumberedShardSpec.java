package com.metamx.druid.shard;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.partition.NumberedPartitionChunk;
import com.metamx.druid.partition.PartitionChunk;

import java.util.Map;

public class NumberedShardSpec implements ShardSpec
{
  @JsonIgnore
  final private int partitionNum;

  @JsonIgnore
  final private int partitions;

  @JsonCreator
  public NumberedShardSpec(
      @JsonProperty("partitionNum") int partitionNum,
      @JsonProperty("partitions") int partitions
  )
  {
    Preconditions.checkArgument(partitionNum >= 0, "partitionNum >= 0");
    Preconditions.checkArgument(partitionNum < partitions, "partitionNum < partitions");
    this.partitionNum = partitionNum;
    this.partitions = partitions;
  }

  @JsonProperty("partitionNum")
  @Override
  public int getPartitionNum()
  {
    return partitionNum;
  }

  @JsonProperty("partitions")
  public int getPartitions()
  {
    return partitions;
  }

  @Override
  public <T> PartitionChunk<T> createChunk(T obj)
  {
    return NumberedPartitionChunk.make(partitionNum, partitions, obj);
  }

  @Override
  public boolean isInChunk(Map<String, String> dimensions)
  {
    return true;
  }

  @Override
  public boolean isInChunk(InputRow inputRow)
  {
    return true;
  }
}
