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

package io.druid.indexer.hadoop;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.druid.common.utils.JodaUtils;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.query.filter.DimFilter;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.util.List;

public class DatasourceIngestionSpec
{
  private final String dataSource;
  private final List<Interval> intervals;
  private final List<DataSegment> segments;
  private final DimFilter filter;
  private final List<String> dimensions;
  private final List<String> metrics;
  private final boolean ignoreWhenNoSegments;

  @JsonCreator
  public DatasourceIngestionSpec(
      @JsonProperty("dataSource") String dataSource,
      @Deprecated @JsonProperty("interval") Interval interval,
      @JsonProperty("intervals") List<Interval> intervals,
      @JsonProperty("segments") List<DataSegment> segments,
      @JsonProperty("filter") DimFilter filter,
      @JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("metrics") List<String> metrics,
      @JsonProperty("ignoreWhenNoSegments") boolean ignoreWhenNoSegments
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
        ignoreWhenNoSegments
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
        ignoreWhenNoSegments
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
        ignoreWhenNoSegments
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
        ignoreWhenNoSegments
    );
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DatasourceIngestionSpec that = (DatasourceIngestionSpec) o;

    if (ignoreWhenNoSegments != that.ignoreWhenNoSegments) {
      return false;
    }
    if (!dataSource.equals(that.dataSource)) {
      return false;
    }
    if (!intervals.equals(that.intervals)) {
      return false;
    }
    if (segments != null ? !segments.equals(that.segments) : that.segments != null) {
      return false;
    }
    if (filter != null ? !filter.equals(that.filter) : that.filter != null) {
      return false;
    }
    if (dimensions != null ? !dimensions.equals(that.dimensions) : that.dimensions != null) {
      return false;
    }
    return !(metrics != null ? !metrics.equals(that.metrics) : that.metrics != null);

  }

  @Override
  public int hashCode()
  {
    int result = dataSource.hashCode();
    result = 31 * result + intervals.hashCode();
    result = 31 * result + (segments != null ? segments.hashCode() : 0);
    result = 31 * result + (filter != null ? filter.hashCode() : 0);
    result = 31 * result + (dimensions != null ? dimensions.hashCode() : 0);
    result = 31 * result + (metrics != null ? metrics.hashCode() : 0);
    result = 31 * result + (ignoreWhenNoSegments ? 1 : 0);
    return result;
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
           '}';
  }
}
