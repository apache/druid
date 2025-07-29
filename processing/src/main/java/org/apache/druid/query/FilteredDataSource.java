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
import org.apache.druid.segment.FilteredSegment;
import org.apache.druid.segment.SegmentMapFunction;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * This class models a data source to be unnested which is present along with a filter.
 * An example for this data source follows:
 *
 * Consider this query:
 * SELECT d3 FROM (select * from druid.numfoo where dim2='a'), UNNEST(MV_TO_ARRAY(dim3))
 *
 * where the filter data source has numFoo as base and dim2='a' as the filter
 *
 * Without this data source, the planner was converting the inner query to a query data source
 * putting more work to be done at the broker level. This pushes the operations down to the
 * segments and is more performant.
 */
public class FilteredDataSource implements DataSource
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

  private FilteredDataSource(DataSource base, @Nullable DimFilter filter)
  {
    this.base = base;
    this.filter = filter;
  }

  @JsonCreator
  public static FilteredDataSource create(
      @JsonProperty("base") DataSource base,
      @JsonProperty("filter") @Nullable DimFilter f
  )
  {
    return new FilteredDataSource(base, f);
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

    return new FilteredDataSource(children.get(0), filter);
  }

  @Override
  public boolean isCacheable(boolean isBroker)
  {
    return false;
  }

  @Override
  public boolean isGlobal()
  {
    return base.isGlobal();
  }

  @Override
  public boolean isProcessable()
  {
    return base.isProcessable();
  }

  @Override
  public SegmentMapFunction createSegmentMapFunction(Query query)
  {
    return base.createSegmentMapFunction(query).thenMap(segment -> new FilteredSegment(segment, filter));
  }

  @Override
  public String toString()
  {
    return "FilteredDataSource{" +
           "base=" + base +
           ", filter='" + filter + '\'' +
           '}';
  }

  @Override
  public byte[] getCacheKey()
  {
    return null;
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
    FilteredDataSource that = (FilteredDataSource) o;
    return Objects.equals(base, that.base) && Objects.equals(filter, that.filter);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(base, filter);
  }
}
