package com.metamx.druid.shard;

import com.metamx.druid.input.InputRow;
import com.metamx.druid.partition.PartitionChunk;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

import java.util.Map;

/**
 * A Marker interface that exists to combine ShardSpec objects together for Jackson
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, property="type", include=JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
    @JsonSubTypes.Type(name="single", value=SingleDimensionShardSpec.class),
    @JsonSubTypes.Type(name="none", value=NoneShardSpec.class)
})
public interface ShardSpec
{
  public <T> PartitionChunk<T> createChunk(T obj);
  public boolean isInChunk(Map<String, String> dimensions);
  public boolean isInChunk(InputRow inputRow);
  public int getPartitionNum();
}
