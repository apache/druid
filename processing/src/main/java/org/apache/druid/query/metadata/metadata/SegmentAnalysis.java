/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.metadata.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SegmentAnalysis implements Comparable<SegmentAnalysis>
{
  /**
   * Segment id is stored as a String rather than {@link org.apache.druid.timeline.SegmentId}, because when a
   * SegmentAnalysis object is sent across Druid nodes, on the reciever (deserialization) side it's impossible to
   * unambiguously convert a segment id string (as transmitted in the JSON format) back into a {@code SegmentId} object
   * ({@link org.apache.druid.timeline.SegmentId#tryParse} javadoc explains that ambiguities in details). It would be
   * fine to have the type of this field of Object, setting it to {@code SegmentId} on the sender side and remaining as
   * a String on the reciever side, but it's even less type-safe than always storing the segment id as a String.
   */
  private final String id;
  private final List<Interval> interval;
  private final Map<String, ColumnAnalysis> columns;
  private final long size;
  private final long numRows;
  private final Map<String, AggregatorFactory> aggregators;
  private final TimestampSpec timestampSpec;
  private final Granularity queryGranularity;
  private final Boolean rollup;

  @JsonCreator
  public SegmentAnalysis(
      @JsonProperty("id") String id,
      @JsonProperty("intervals") List<Interval> interval,
      @JsonProperty("columns") Map<String, ColumnAnalysis> columns,
      @JsonProperty("size") long size,
      @JsonProperty("numRows") long numRows,
      @JsonProperty("aggregators") Map<String, AggregatorFactory> aggregators,
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("queryGranularity") Granularity queryGranularity,
      @JsonProperty("rollup") Boolean rollup
  )
  {
    this.id = id;
    this.interval = interval;
    this.columns = columns;
    this.size = size;
    this.numRows = numRows;
    this.aggregators = aggregators;
    this.timestampSpec = timestampSpec;
    this.queryGranularity = queryGranularity;
    this.rollup = rollup;
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
  public long getNumRows()
  {
    return numRows;
  }

  @JsonProperty
  public TimestampSpec getTimestampSpec()
  {
    return timestampSpec;
  }

  @JsonProperty
  public Granularity getQueryGranularity()
  {
    return queryGranularity;
  }

  @JsonProperty
  public Boolean isRollup()
  {
    return rollup;
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
           ", timestampSpec=" + timestampSpec +
           ", queryGranularity=" + queryGranularity +
           ", rollup=" + rollup +
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
           Objects.equals(rollup, that.rollup) &&
           Objects.equals(id, that.id) &&
           Objects.equals(interval, that.interval) &&
           Objects.equals(columns, that.columns) &&
           Objects.equals(aggregators, that.aggregators) &&
           Objects.equals(timestampSpec, that.timestampSpec) &&
           Objects.equals(queryGranularity, that.queryGranularity);
  }

  /**
   * Best-effort hashCode method; relies on AggregatorFactory.hashCode, which is not guaranteed to be sanely
   * implemented.
   */
  @Override
  public int hashCode()
  {
    return Objects.hash(id, interval, columns, size, numRows, aggregators, timestampSpec, queryGranularity, rollup);
  }

  @Override
  public int compareTo(SegmentAnalysis rhs)
  {
    return id.compareTo(rhs.getId());
  }
}
