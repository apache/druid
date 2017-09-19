/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.timeline.partition;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;

import io.druid.data.input.InputRow;
import io.druid.java.util.common.ISE;

import java.util.List;
import java.util.Map;

/**
 * Class uses getters/setters to work around http://jira.codehaus.org/browse/MSHADE-92
 *
 * Adjust to JsonCreator and final fields when resolved.
 */
public class SingleDimensionShardSpec implements ShardSpec
{
  private String dimension;
  private String start;
  private String end;
  private int partitionNum;

  public SingleDimensionShardSpec()
  {
    this(null, null, null, -1);
  }

  public SingleDimensionShardSpec(
      String dimension,
      String start,
      String end,
      int partitionNum
  )
  {
    this.dimension = dimension;
    this.start = start;
    this.end = end;
    this.partitionNum = partitionNum;
  }

  @JsonProperty("dimension")
  public String getDimension()
  {
    return dimension;
  }

  public void setDimension(String dimension)
  {
    this.dimension = dimension;
  }

  @JsonProperty("start")
  public String getStart()
  {
    return start;
  }

  public void setStart(String start)
  {
    this.start = start;
  }

  @JsonProperty("end")
  public String getEnd()
  {
    return end;
  }

  public void setEnd(String end)
  {
    this.end = end;
  }

  @Override
  @JsonProperty("partitionNum")
  public int getPartitionNum()
  {
    return partitionNum;
  }

  @Override
  public ShardSpecLookup getLookup(final List<ShardSpec> shardSpecs)
  {
    return new ShardSpecLookup()
    {
      @Override
      public ShardSpec getShardSpec(long timestamp, InputRow row)
      {
        for (ShardSpec spec : shardSpecs) {
          if (spec.isInChunk(timestamp, row)) {
            return spec;
          }
        }
        throw new ISE("row[%s] doesn't fit in any shard[%s]", row, shardSpecs);
      }
    };
  }

  @Override
  public Map<String, Range<String>> getDomain()
  {
    Range<String> range;
    if (start == null && end == null) {
      range = Range.all();
    } else if (start == null) {
      range = Range.atMost(end);
    } else if (end == null) {
      range = Range.atLeast(start);
    } else {
      range = Range.closed(start, end);
    }
    return ImmutableMap.of(dimension, range);
  }

  public void setPartitionNum(int partitionNum)
  {
    this.partitionNum = partitionNum;
  }

  @Override
  public <T> PartitionChunk<T> createChunk(T obj)
  {
    return new StringPartitionChunk<T>(start, end, partitionNum, obj);
  }

  @Override
  public boolean isInChunk(long timestamp, InputRow inputRow)
  {
    final List<String> values = inputRow.getDimension(dimension);

    if (values == null || values.size() != 1) {
      return checkValue(null);
    } else {
      return checkValue(values.get(0));
    }
  }

  private boolean checkValue(String value)
  {
    if (value == null) {
      return start == null;
    }

    if (start == null) {
      return end == null || value.compareTo(end) < 0;
    }

    return value.compareTo(start) >= 0 &&
           (end == null || value.compareTo(end) < 0);
  }

  @Override
  public String toString()
  {
    return "SingleDimensionShardSpec{" +
           "dimension='" + dimension + '\'' +
           ", start='" + start + '\'' +
           ", end='" + end + '\'' +
           ", partitionNum=" + partitionNum +
           '}';
  }
}
