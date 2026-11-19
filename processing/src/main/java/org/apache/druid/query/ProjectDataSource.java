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
import org.apache.druid.query.filter.SegmentPruner;
import org.apache.druid.segment.ProjectSegment;
import org.apache.druid.segment.SegmentMapFunction;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a projection over an underlying datasource. The projection includes all base columns, plus a
 * set of virtual columns given by {@link #getVirtualColumns()}.
 */
public class ProjectDataSource implements DataSource
{
  private final DataSource base;
  private final VirtualColumns virtualColumns;

  @JsonCreator
  public ProjectDataSource(
      @JsonProperty("base") final DataSource base,
      @JsonProperty("virtualColumns") final VirtualColumns virtualColumns
  )
  {
    this.base = Objects.requireNonNull(base, "base");
    this.virtualColumns = VirtualColumns.nullToEmpty(virtualColumns);
  }

  @JsonProperty("base")
  public DataSource getBase()
  {
    return base;
  }

  @JsonProperty("virtualColumns")
  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
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
    final DataSource child = CollectionUtils.getOnlyElement(
        children,
        xs -> new IAE("Expected [1] child, got [%d]", children.size())
    );

    return new ProjectDataSource(child, virtualColumns);
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
    return base.createSegmentMapFunction(query).thenMap(segment -> new ProjectSegment(segment, virtualColumns));
  }

  @Nullable
  @Override
  public SegmentPruner createSegmentPruner(@Nullable DimFilter filter, VirtualColumns virtualColumns)
  {
    // Our virtual columns are applied beneath the incoming ones.
    return base.createSegmentPruner(filter, this.virtualColumns.plus(virtualColumns));
  }

  @Override
  public Set<String> getReservedColumns()
  {
    final Set<String> names = new HashSet<>(base.getReservedColumns());
    for (final VirtualColumn virtualColumn : virtualColumns.getVirtualColumns()) {
      names.add(virtualColumn.getOutputName());
      names.addAll(virtualColumn.requiredColumns());
    }
    return names;
  }

  @Override
  public boolean mayFilterRows()
  {
    return base.mayFilterRows();
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
    ProjectDataSource that = (ProjectDataSource) o;
    return Objects.equals(base, that.base) && Objects.equals(virtualColumns, that.virtualColumns);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(base, virtualColumns);
  }

  @Override
  public String toString()
  {
    return "ProjectDataSource{" +
           "base=" + base +
           ", virtualColumns=" + virtualColumns +
           '}';
  }
}
