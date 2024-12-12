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
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.TrueDimFilter;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.segment.RestrictedSegment;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.utils.JvmUtils;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Reperesents a TableDataSource with row-level restriction.
 * <p>
 * A RestrictedDataSource means the base TableDataSource has restriction imposed. A table without any restriction should
 * never be transformed to a RestrictedDataSource. Druid internal system and admin users would have a null rowFilter,
 * while external users would have a rowFilter based on the applied restriction.
 */
public class RestrictedDataSource implements DataSource
{
  private final TableDataSource base;
  @Nullable
  private final DimFilter rowFilter;

  @JsonProperty("base")
  public TableDataSource getBase()
  {
    return base;
  }

  /**
   * Returns true if the row-level filter imposes no restrictions.
   */
  public boolean allowAll()
  {
    return Objects.isNull(rowFilter) || rowFilter.equals(TrueDimFilter.instance());
  }

  @Nullable
  @JsonProperty("filter")
  public DimFilter getFilter()
  {
    return rowFilter;
  }

  RestrictedDataSource(TableDataSource base, @Nullable DimFilter rowFilter)
  {
    this.base = base;
    this.rowFilter = rowFilter;
  }

  @JsonCreator
  public static RestrictedDataSource create(
      @JsonProperty("base") DataSource base,
      @Nullable @JsonProperty("filter") DimFilter rowFilter
  )
  {
    if (!(base instanceof TableDataSource)) {
      throw new IAE("Expected a TableDataSource, got [%s]", base.getClass());
    }
    return new RestrictedDataSource((TableDataSource) base, rowFilter);
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

    return RestrictedDataSource.create(children.get(0), rowFilter);
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
  public Function<SegmentReference, SegmentReference> createSegmentMapFunction(
      Query query,
      AtomicLong cpuTimeAccumulator
  )
  {
    return JvmUtils.safeAccumulateThreadCpuTime(
        cpuTimeAccumulator,
        () -> base.createSegmentMapFunction(
            query,
            cpuTimeAccumulator
        ).andThen((segment) -> (new RestrictedSegment(segment, rowFilter)))
    );
  }

  @Override
  public DataSource withUpdatedDataSource(DataSource newSource)
  {
    return RestrictedDataSource.create(newSource, rowFilter);
  }

  @Override
  public DataSource mapWithRestriction(Map<String, Optional<DimFilter>> rowFilters, boolean enableStrictPolicyCheck)
  {
    if (!rowFilters.containsKey(this.base.getName()) && enableStrictPolicyCheck) {
      throw DruidException.defensive("Missing row filter for table [%s]", this.base.getName());
    }

    Optional<DimFilter> newFilter = rowFilters.getOrDefault(this.base.getName(), Optional.empty());
    if (!newFilter.isPresent()) {
      throw DruidException.defensive(
          "No restriction found on table [%s], but had %s before.",
          this.base.getName(),
          this.rowFilter
      );
    }
    if (newFilter.get().equals(TrueDimFilter.instance())) {
      // The internal druid_system always has a TrueDimFilter, whic can be applied in conjunction with an external user's filter.
      return this;
    } else if (newFilter.get().equals(rowFilter)) {
      // This likely occurs when we perform an authentication check for the same user more than once, which is not ideal.
      return this;
    } else {
      throw new ISE("Incompatible restrictions on [%s]: %s and %s", this.base.getName(), rowFilter, newFilter.get());
    }
  }

  @Override
  public String toString()
  {
    try {
      return "RestrictedDataSource{" +
             "base=" + base +
             ", filter='" + rowFilter + '}';
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
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
    RestrictedDataSource that = (RestrictedDataSource) o;
    return Objects.equals(base, that.base) && Objects.equals(rowFilter, that.rowFilter);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(base, rowFilter);
  }
}
