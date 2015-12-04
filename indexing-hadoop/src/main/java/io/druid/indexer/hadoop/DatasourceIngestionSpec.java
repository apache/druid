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
import io.druid.granularity.QueryGranularity;
import io.druid.query.filter.DimFilter;
import org.joda.time.Interval;

import java.util.List;

public class DatasourceIngestionSpec
{
  private final String dataSource;
  private final List<Interval> intervals;
  private final DimFilter filter;
  private final QueryGranularity granularity;
  private final List<String> dimensions;
  private final List<String> metrics;

  @JsonCreator
  public DatasourceIngestionSpec(
      @JsonProperty("dataSource") String dataSource,
      @Deprecated @JsonProperty("interval") Interval interval,
      @JsonProperty("intervals") List<Interval> intervals,
      @JsonProperty("filter") DimFilter filter,
      @JsonProperty("granularity") QueryGranularity granularity,
      @JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("metrics") List<String> metrics
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

    this.filter = filter;
    this.granularity = granularity == null ? QueryGranularity.NONE : granularity;

    this.dimensions = dimensions;
    this.metrics = metrics;
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
  public DimFilter getFilter()
  {
    return filter;
  }

  @JsonProperty
  public QueryGranularity getGranularity()
  {
    return granularity;
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

  public DatasourceIngestionSpec withDimensions(List<String> dimensions)
  {
    return new DatasourceIngestionSpec(dataSource, null, intervals, filter, granularity, dimensions, metrics);
  }

  public DatasourceIngestionSpec withMetrics(List<String> metrics)
  {
    return new DatasourceIngestionSpec(dataSource, null, intervals, filter, granularity, dimensions, metrics);
  }

  public DatasourceIngestionSpec withQueryGranularity(QueryGranularity granularity)
  {
    return new DatasourceIngestionSpec(dataSource, null, intervals, filter, granularity, dimensions, metrics);
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

    if (!dataSource.equals(that.dataSource)) {
      return false;
    }
    if (!intervals.equals(that.intervals)) {
      return false;
    }
    if (filter != null ? !filter.equals(that.filter) : that.filter != null) {
      return false;
    }
    if (!granularity.equals(that.granularity)) {
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
    result = 31 * result + (filter != null ? filter.hashCode() : 0);
    result = 31 * result + granularity.hashCode();
    result = 31 * result + (dimensions != null ? dimensions.hashCode() : 0);
    result = 31 * result + (metrics != null ? metrics.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "DatasourceIngestionSpec{" +
           "dataSource='" + dataSource + '\'' +
           ", intervals=" + intervals +
           ", filter=" + filter +
           ", granularity=" + granularity +
           ", dimensions=" + dimensions +
           ", metrics=" + metrics +
           '}';
  }
}
