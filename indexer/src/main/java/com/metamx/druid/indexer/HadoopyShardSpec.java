package com.metamx.druid.indexer;

import com.metamx.druid.shard.ShardSpec;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * ShardSpec + a shard ID that is unique across this run. The shard ID is used for grouping and partitioning.
 * There is one HadoopyShardSpec per Bucket, and our shardNum matches the Bucket shardNum.
 */
public class HadoopyShardSpec
{
  private final ShardSpec actualSpec;
  private final int shardNum;

  @JsonCreator
  public HadoopyShardSpec(
      @JsonProperty("actualSpec") ShardSpec actualSpec,
      @JsonProperty("shardNum") int shardNum
  )
  {
    this.actualSpec = actualSpec;
    this.shardNum = shardNum;
  }

  @JsonProperty
  public ShardSpec getActualSpec()
  {
    return actualSpec;
  }

  @JsonProperty
  public int getShardNum()
  {
    return shardNum;
  }

  @Override
  public String toString()
  {
    return "HadoopyShardSpec{" +
           "actualSpec=" + actualSpec +
           ", shardNum=" + shardNum +
           '}';
  }
}
