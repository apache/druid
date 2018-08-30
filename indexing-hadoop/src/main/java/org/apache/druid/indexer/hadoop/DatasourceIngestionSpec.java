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

package org.apache.druid.indexer.hadoop;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.util.List;
import java.util.Objects;

public class DatasourceIngestionSpec
{
  private final String dataSource;
  private final List<Interval> intervals;
  private final List<DataSegment> segments;
  private final DimFilter filter;
  private final List<String> dimensions;
  private final List<String> metrics;
  private final boolean ignoreWhenNoSegments;

  // Note that the only purpose of the transformSpec field is to hold the value from the overall dataSchema.
  // It is not meant to be provided by end users, and will be overwritten.
  private final TransformSpec transformSpec;

  @JsonCreator
  public DatasourceIngestionSpec(
      @JsonProperty("dataSource") String dataSource,
      @Deprecated @JsonProperty("interval") Interval interval,
      @JsonProperty("intervals") List<Interval> intervals,
      @JsonProperty("segments") List<DataSegment> segments,
      @JsonProperty("filter") DimFilter filter,
      @JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("metrics") List<String> metrics,
      @JsonProperty("ignoreWhenNoSegments") boolean ignoreWhenNoSegments,
      @JsonProperty("transformSpec") TransformSpec transformSpec
  )
  {
    this.dataSource = Preconditions.checkNotNull(dataSource, "null dataSource");

    Preconditions.checkArgument(
        interval == null || intervals == null,
        "please specify intervals only"
    );

    List<Interval> theIntervals = null;
    if (interval != null) {
      theIntervals = ImmutableList.of(interval);
    } else if (intervals != null && intervals.size() > 0) {
      theIntervals = JodaUtils.condenseIntervals(intervals);
    }
    this.intervals = Preconditions.checkNotNull(theIntervals, "no intervals found");

    // note that it is important to have intervals even if user explicitly specifies the list of
    // segments, because segment list's min/max boundaries might not align the intended interval
    // to read in all cases.
    this.segments = segments;

    this.filter = filter;
    this.dimensions = dimensions;
    this.metrics = metrics;

    this.ignoreWhenNoSegments = ignoreWhenNoSegments;
    this.transformSpec = transformSpec != null ? transformSpec : TransformSpec.NONE;
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public List<Interval> getIntervals()
  {
    return intervals;
  }

  @JsonProperty
  public List<DataSegment> getSegments()
  {
    return segments;
  }

  @JsonProperty
  public DimFilter getFilter()
  {
    return filter;
  }

  @JsonProperty
  public List<String> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty
  public List<String> getMetrics()
  {
    return metrics;
  }

  @JsonProperty
  public boolean isIgnoreWhenNoSegments()
  {
    return ignoreWhenNoSegments;
  }

  @JsonProperty
  public TransformSpec getTransformSpec()
  {
    return transformSpec;
  }

  public DatasourceIngestionSpec withDimensions(List<String> dimensions)
  {
    return new DatasourceIngestionSpec(
        dataSource,
        null,
        intervals,
        segments,
        filter,
        dimensions,
        metrics,
        ignoreWhenNoSegments,
        transformSpec
    );
  }

  public DatasourceIngestionSpec withMetrics(List<String> metrics)
  {
    return new DatasourceIngestionSpec(
        dataSource,
        null,
        intervals,
        segments,
        filter,
        dimensions,
        metrics,
        ignoreWhenNoSegments,
        transformSpec
    );
  }

  public DatasourceIngestionSpec withQueryGranularity(Granularity granularity)
  {
    return new DatasourceIngestionSpec(
        dataSource,
        null,
        intervals,
        segments,
        filter,
        dimensions,
        metrics,
        ignoreWhenNoSegments,
        transformSpec
    );
  }

  public DatasourceIngestionSpec withIgnoreWhenNoSegments(boolean ignoreWhenNoSegments)
  {
    return new DatasourceIngestionSpec(
        dataSource,
        null,
        intervals,
        segments,
        filter,
        dimensions,
        metrics,
        ignoreWhenNoSegments,
        transformSpec
    );
  }

  public DatasourceIngestionSpec withTransformSpec(TransformSpec transformSpec)
  {
    return new DatasourceIngestionSpec(
        dataSource,
        null,
        intervals,
        segments,
        filter,
        dimensions,
        metrics,
        ignoreWhenNoSegments,
        transformSpec
    );
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final DatasourceIngestionSpec that = (DatasourceIngestionSpec) o;
    return ignoreWhenNoSegments == that.ignoreWhenNoSegments &&
           Objects.equals(dataSource, that.dataSource) &&
           Objects.equals(intervals, that.intervals) &&
           Objects.equals(segments, that.segments) &&
           Objects.equals(filter, that.filter) &&
           Objects.equals(dimensions, that.dimensions) &&
           Objects.equals(metrics, that.metrics) &&
           Objects.equals(transformSpec, that.transformSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        dataSource,
        intervals,
        segments,
        filter,
        dimensions,
        metrics,
        ignoreWhenNoSegments,
        transformSpec
    );
  }

  @Override
  public String toString()
  {
    return "DatasourceIngestionSpec{" +
           "dataSource='" + dataSource + '\'' +
           ", intervals=" + intervals +
           ", segments=" + segments +
           ", filter=" + filter +
           ", dimensions=" + dimensions +
           ", metrics=" + metrics +
           ", ignoreWhenNoSegments=" + ignoreWhenNoSegments +
           ", transformSpec=" + transformSpec +
           '}';
  }
}
