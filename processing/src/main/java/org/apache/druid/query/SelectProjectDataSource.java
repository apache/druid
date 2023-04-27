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
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.SelectProjectSegmentReference;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.utils.JvmUtils;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class SelectProjectDataSource implements DataSource
{
  private final DataSource base;
  private final VirtualColumns virtualColumns;

  @JsonProperty("base")
  public DataSource getBase()
  {
    return base;
  }

  @Override
  public String toString()
  {
    return "SelectProjectDataSource{" +
           "base=" + base +
           ", virtualColumns=" + virtualColumns +
           '}';
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
    SelectProjectDataSource that = (SelectProjectDataSource) o;
    return base.equals(that.base) && virtualColumns.equals(that.virtualColumns);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(base, virtualColumns);
  }

  @JsonProperty("virtualColumns")
  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
  }

  private SelectProjectDataSource(DataSource base, VirtualColumns vc)
  {
    this.base = base;
    this.virtualColumns = vc;
  }

  @JsonCreator
  public static SelectProjectDataSource create(
      @JsonProperty("base") DataSource baseDataSource,
      @JsonProperty("virtualColumns") VirtualColumns vc
  )
  {
    return new SelectProjectDataSource(baseDataSource, vc);
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

    return new SelectProjectDataSource(children.get(0), virtualColumns);
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
                new SelectProjectSegmentReference(
                    segmentMapFn.apply(baseSegment),
                    virtualColumns
                )
    );
  }

  @Override
  public DataSource withUpdatedDataSource(DataSource newSource)
  {
    return new SelectProjectDataSource(newSource, virtualColumns);
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
}
