package com.metamx.druid.shard;

import com.metamx.druid.input.InputRow;
import com.metamx.druid.partition.PartitionChunk;
import com.metamx.druid.partition.SingleElementPartitionChunk;
import org.codehaus.jackson.annotate.JsonTypeName;

import java.util.Map;

/**
 */
public class NoneShardSpec implements ShardSpec
{
  @Override
  public <T> PartitionChunk<T> createChunk(T obj)
  {
    return new SingleElementPartitionChunk<T>(obj);
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

  @Override
  public int getPartitionNum()
  {
    return 0;
  }

  @Override
  public boolean equals(Object obj)
  {
    return obj instanceof NoneShardSpec;
  }

  @Override
  public String toString()
  {
    return "NoneShardSpec";
  }
}
