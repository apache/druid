package com.metamx.druid.shard;

import com.metamx.druid.input.InputRow;
import com.metamx.druid.partition.LinearPartitionChunk;
import com.metamx.druid.partition.PartitionChunk;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: jbae
 * Date: 2/4/13
 * Time: 10:22 AM
 * To change this template use File | Settings | File Templates.
 */
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