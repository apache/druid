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

package org.apache.druid.query.metadata;

import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.metadata.metadata.ColumnAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;
import org.apache.druid.segment.AggregateProjectionMetadata;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Helper class to build {@link SegmentAnalysis} objects for testing purposes.
 */
public class SegmentAnalysisBuilder
{
  String segmentId;
  List<Interval> intervals = null;
  LinkedHashMap<String, ColumnAnalysis> columns = new LinkedHashMap<>();
  Map<String, AggregatorFactory> aggregators = new LinkedHashMap<>();
  Map<String, AggregateProjectionMetadata> projections = new LinkedHashMap<>();
  Optional<Integer> size = Optional.empty();
  Optional<Integer> numRows = Optional.empty();
  Optional<Boolean> rollup = Optional.empty();

  SegmentAnalysisBuilder(String segmentId)
  {
    this.segmentId = segmentId;
  }

  SegmentAnalysisBuilder(SegmentId segmentId)
  {
    this.segmentId = segmentId.toString();
  }

  SegmentAnalysisBuilder size(int size)
  {
    if (this.size.isEmpty()) {
      this.size = Optional.of(size);
    } else {
      throw new IllegalStateException("Size is already set: " + this.size.get());
    }
    return this;
  }

  SegmentAnalysisBuilder numRows(int numRows)
  {
    if (this.numRows.isEmpty()) {
      this.numRows = Optional.of(numRows);
    } else {
      throw new IllegalStateException("NumRows is already set: " + this.numRows.get());
    }
    return this;
  }

  SegmentAnalysisBuilder rollup(boolean rollup)
  {
    if (this.rollup.isEmpty()) {
      this.rollup = Optional.of(rollup);
    } else {
      throw new IllegalStateException("Rollup is already set: " + this.rollup.get());
    }
    return this;
  }

  SegmentAnalysisBuilder interval(Interval interval)
  {
    if (this.intervals == null) {
      this.intervals = new ArrayList<>();
    }
    this.intervals.add(interval);
    return this;
  }

  SegmentAnalysisBuilder column(String columnName, ColumnAnalysis columnAnalysis)
  {
    this.columns.put(columnName, columnAnalysis);
    return this;
  }

  SegmentAnalysisBuilder aggregator(String name, AggregatorFactory aggregatorFactory)
  {
    this.aggregators.put(name, aggregatorFactory);
    return this;
  }

  SegmentAnalysisBuilder projection(String name, AggregateProjectionMetadata projection)
  {
    this.projections.put(name, projection);
    return this;
  }

  SegmentAnalysis build()
  {
    return new SegmentAnalysis(
        segmentId,
        intervals,
        columns,
        size.orElse(0),
        numRows.orElse(0),
        aggregators.isEmpty() ? null : aggregators,
        projections.isEmpty() ? null : projections,
        null,
        null,
        rollup.orElse(null)
    );
  }
}
