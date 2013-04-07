package com.metamx.druid.shard;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.partition.LinearPartitionChunk;
import com.metamx.druid.partition.PartitionChunk;

import java.util.Map;

public class LinearShardSpec implements ShardSpec {
  private int partitionNum;

  public LinearShardSpec() {
    this(-1);
  }

  public LinearShardSpec(int partitionNum) {
    this.partitionNum = partitionNum;
  }

  @JsonProperty("partitionNum")
  @Override
  public int getPartitionNum() {
    return partitionNum;
  }

  @Override
  public <T> PartitionChunk<T> createChunk(T obj) {
    return new LinearPartitionChunk<T>(partitionNum, obj);
  }

  @Override
  public boolean isInChunk(Map<String, String> dimensions) {
    return true;
  }

  @Override
  public boolean isInChunk(InputRow inputRow) {
    return true;
  }
}
