/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.timeline.partition;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.ISE;
import io.druid.data.input.InputRow;

import java.util.List;

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
