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

package org.apache.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.segment.FilterSegmentReference;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.utils.JvmUtils;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class FilterDataSource implements DataSource
{

  private final DataSource base;
  private final DimFilter filter;

  @JsonProperty("base")
  public DataSource getBase()
  {
    return base;
  }

  @JsonProperty("filter")
  public DimFilter getFilter()
  {
    return filter;
  }

  private FilterDataSource(DataSource base, DimFilter filter)
  {
    this.base = base;
    this.filter = filter;
  }

  @JsonCreator
  public static FilterDataSource create(
      @JsonProperty("base") DataSource base,
      @JsonProperty("filter") DimFilter f
  )
  {
    return new FilterDataSource(base, f);
  }

  @Override
  public Set<String> getTableNames()
  {
    return base.getTableNames();
  }

  @Override
  public List<DataSource> getChildren()
  {
    return ImmutableList.of(base);
  }

  @Override
  public DataSource withChildren(List<DataSource> children)
  {
    if (children.size() != 1) {
      throw new IAE("Expected [1] child, got [%d]", children.size());
    }

    return new FilterDataSource(children.get(0), filter);
  }

  @Override
  public boolean isCacheable(boolean isBroker)
  {
    return false;
  }

  @Override
  public boolean isGlobal()
  {
    return false;
  }

  @Override
  public boolean isConcrete()
  {
    return false;
  }

  @Override
  public Function<SegmentReference, SegmentReference> createSegmentMapFunction(
      Query query,
      AtomicLong cpuTimeAccumulator
  )
  {
    final Function<SegmentReference, SegmentReference> segmentMapFn = base.createSegmentMapFunction(
        query,
        cpuTimeAccumulator
    );
    return JvmUtils.safeAccumulateThreadCpuTime(
        cpuTimeAccumulator,
        () ->
            baseSegment ->
                new FilterSegmentReference(
                    segmentMapFn.apply(baseSegment),
                    filter
                )
    );
  }

  @Override
  public DataSource withUpdatedDataSource(DataSource newSource)
  {
    return new FilterDataSource(newSource, filter);
  }

  @Override
  public String toString()
  {
    return "FilterDataSource{" +
           "base=" + base +
           ", filter='" + filter + '\'' +
           '}';
  }

  @Override
  public byte[] getCacheKey()
  {
    return new byte[0];
  }

  @Override
  public DataSourceAnalysis getAnalysis()
  {
    final DataSource current = this.getBase();
    return current.getAnalysis();
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
    FilterDataSource that = (FilterDataSource) o;
    return Objects.equals(base, that.base) && Objects.equals(filter, that.filter);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(base, filter);
  }
}
