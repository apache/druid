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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.segment.FilteredSegment;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.VirtualColumns;
import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

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
@JsonInclude(Include.NON_DEFAULT)
public class FilteredDataSource implements DataSource
{
  private final DataSource base;
  private final DimFilter filter;
  private final VirtualColumns virtualColumns;

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

  @JsonProperty("virtualColumns")
  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
  }

  // To provide defaults for Jackson
  private FilteredDataSource()
  {
    this(null, null, null);
  }

  private FilteredDataSource(DataSource base, @Nullable DimFilter filter, VirtualColumns virtualColumns)
  {
    this.base = base;
    this.filter = filter;
    this.virtualColumns = virtualColumns == null ? VirtualColumns.EMPTY : virtualColumns;
  }

  @JsonCreator
  public static FilteredDataSource create(
      @JsonProperty("base") DataSource base,
      @JsonProperty("filter") @Nullable DimFilter filter,
      @JsonProperty("virtualColumns") @Nullable VirtualColumns virtualColumns
  )
  {
    return new FilteredDataSource(base, filter, virtualColumns);
  }

  public static FilteredDataSource create(
      DataSource base,
      @Nullable DimFilter filter
  )
  {
    return Druids.filteredDataSource(base, filter);
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

    return new FilteredDataSource(children.get(0), filter, virtualColumns);
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
  public boolean isConcrete()
  {
    return base.isConcrete();
  }

  @Override
  public Function<SegmentReference, SegmentReference> createSegmentMapFunction(SegmentMapConfig cfg)
  {
    SegmentMapConfig newCfg = cfg
        .withVirtualColumns(virtualColumns);

    final Function<SegmentReference, SegmentReference> segmentMapFn = base.createSegmentMapFunction(
        newCfg
    );
    return baseSegment -> new FilteredSegment(segmentMapFn.apply(baseSegment), filter, virtualColumns);
  }

  @Override
  public DataSource withUpdatedDataSource(DataSource newSource)
  {
    return new FilteredDataSource(newSource, filter, virtualColumns);
  }

  @Override
  public String toString()
  {
    return "FilteredDataSource{" +
        "base=" + base +
        ", filter='" + filter + "'" +
        ", virtualColumns=" + virtualColumns +
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
    FilteredDataSource that = (FilteredDataSource) o;
    return Objects.equals(base, that.base) &&
        Objects.equals(filter, that.filter) &&
        Objects.equals(virtualColumns, that.virtualColumns);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(base, filter, virtualColumns);
  }
}
