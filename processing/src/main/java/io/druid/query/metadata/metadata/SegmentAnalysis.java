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

package io.druid.query.metadata.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.query.aggregation.AggregatorFactory;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SegmentAnalysis implements Comparable<SegmentAnalysis>
{
  private final String id;
  private final List<Interval> interval;
  private final Map<String, ColumnAnalysis> columns;
  private final long size;
  private final int numRows;
  private final Map<String, AggregatorFactory> aggregators;

  @JsonCreator
  public SegmentAnalysis(
      @JsonProperty("id") String id,
      @JsonProperty("intervals") List<Interval> interval,
      @JsonProperty("columns") Map<String, ColumnAnalysis> columns,
      @JsonProperty("size") long size,
      @JsonProperty("numRows") int numRows,
      @JsonProperty("aggregators") Map<String, AggregatorFactory> aggregators
  )
  {
    this.id = id;
    this.interval = interval;
    this.columns = columns;
    this.size = size;
    this.numRows = numRows;
    this.aggregators = aggregators;
  }

  @JsonProperty
  public String getId()
  {
    return id;
  }

  @JsonProperty
  public List<Interval> getIntervals()
  {
    return interval;
  }

  @JsonProperty
  public Map<String, ColumnAnalysis> getColumns()
  {
    return columns;
  }

  @JsonProperty
  public long getSize()
  {
    return size;
  }

  @JsonProperty
  public int getNumRows()
  {
    return numRows;
  }

  @JsonProperty
  public Map<String, AggregatorFactory> getAggregators()
  {
    return aggregators;
  }

  @Override
  public String toString()
  {
    return "SegmentAnalysis{" +
           "id='" + id + '\'' +
           ", interval=" + interval +
           ", columns=" + columns +
           ", size=" + size +
           ", numRows=" + numRows +
           ", aggregators=" + aggregators +
           '}';
  }

  /**
   * Best-effort equals method; relies on AggregatorFactory.equals, which is not guaranteed to be sanely implemented.
   */
  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SegmentAnalysis that = (SegmentAnalysis) o;
    return size == that.size &&
           numRows == that.numRows &&
           Objects.equals(id, that.id) &&
           Objects.equals(interval, that.interval) &&
           Objects.equals(columns, that.columns) &&
           Objects.equals(aggregators, that.aggregators);
  }

  /**
   * Best-effort hashCode method; relies on AggregatorFactory.hashCode, which is not guaranteed to be sanely
   * implemented.
   */
  @Override
  public int hashCode()
  {
    return Objects.hash(id, interval, columns, size, numRows, aggregators);
  }

  @Override
  public int compareTo(SegmentAnalysis rhs)
  {
    // Nulls first
    if (rhs == null) {
      return 1;
    }
    return id.compareTo(rhs.getId());
  }
}
