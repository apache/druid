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
import org.apache.druid.query.filter.SegmentPruner;
import org.apache.druid.segment.SegmentMapFunction;
import org.apache.druid.segment.VirtualColumns;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Reperesents a UNION ALL of two or more datasources.
 * <p>
 * Native engine can only work with table datasources that are scans or simple mappings (column rename without any
 * expression applied on top). Therefore, it uses methods like {@link #getTableNames()} and
 * {@link #isTableBased()} to assert that the children were TableDataSources.
 * <p>
 * MSQ should be able to plan and work with arbitrary datasources.  It also needs to replace the datasource with the
 * InputNumberDataSource while preparing the query plan.
 */
public class UnionDataSource implements DataSource
{

  @JsonProperty("dataSources")
  private final List<DataSource> dataSources;

  @JsonCreator
  public UnionDataSource(@JsonProperty("dataSources") List<DataSource> dataSources)
  {
    if (dataSources == null || dataSources.isEmpty()) {
      throw new ISE("'dataSources' must be non-null and non-empty for 'union'");
    }

    this.dataSources = dataSources;
  }

  /**
   * Asserts that the children of the union are all table data sources before returning the table names
   */
  @Override
  public Set<String> getTableNames()
  {
    if (!isTableBased()) {
      throw DruidException.defensive("contains non-table based datasource");
    }
    return dataSources
        .stream()
        .map(DataSource::getTableNames)
        .flatMap(Set::stream)
        .collect(Collectors.toSet());
  }

  /**
   * Returns true if this datasource can be ran by the native engine: i.e., every arm is processable as-is and reads
   * nothing but tables.
   */
  public boolean isTableBased()
  {
    return isProcessable() && dataSources.stream().allMatch(UnionDataSource::readsOnlyTables);
  }

  @Override
  public List<DataSource> getChildren()
  {
    return ImmutableList.copyOf(dataSources);
  }

  @Override
  public DataSource withChildren(List<DataSource> children)
  {
    if (children.size() != dataSources.size()) {
      throw new IAE("Expected [%d] children, got [%d]", dataSources.size(), children.size());
    }

    return new UnionDataSource(children);
  }

  @Override
  public boolean isCacheable(boolean isBroker)
  {
    // Disables result-level caching for 'union' datasources, which doesn't work currently.
    // See https://github.com/apache/druid/issues/8713 for reference.
    //
    // Note that per-segment caching is still effective, since at the time the per-segment cache evaluates a query
    // for cacheability, it would have already been rewritten to a query on a single table.
    return false;
  }

  @Override
  public boolean isGlobal()
  {
    return dataSources.stream().allMatch(DataSource::isGlobal);
  }

  @Override
  public boolean isProcessable()
  {
    return dataSources.stream().allMatch(DataSource::isProcessable);
  }

  /**
   * Throws, since each arm reads its own set of segments, so no single function applies to all of them. Callers split
   * a union by arm first; see {@link org.apache.druid.query.planning.ExecutionVertex#splitUnion()}.
   */
  @Override
  public SegmentMapFunction createSegmentMapFunction(Query query)
  {
    throw DruidException.defensive("Cannot map segments of union[%s] without splitting it by arm", dataSources);
  }

  /**
   * Throws, for the same reason as {@link #createSegmentMapFunction}.
   */
  @Override
  public SegmentPruner createSegmentPruner(@Nullable DimFilter filter, VirtualColumns virtualColumns)
  {
    throw DruidException.defensive("Cannot prune segments of union[%s] without splitting it by arm", dataSources);
  }

  @Override
  public Set<String> getReservedColumns()
  {
    final Set<String> names = new HashSet<>();
    for (final DataSource dataSource : dataSources) {
      names.addAll(dataSource.getReservedColumns());
    }
    return names;
  }

  @Override
  public boolean mayFilterRows()
  {
    return dataSources.stream().anyMatch(DataSource::mayFilterRows);
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

    UnionDataSource that = (UnionDataSource) o;

    return dataSources.equals(that.dataSources);
  }

  @Override
  public int hashCode()
  {
    return dataSources.hashCode();
  }

  @Override
  public String toString()
  {
    return "UnionDataSource{" +
           "dataSources=" + dataSources +
           '}';
  }

  /**
   * Whether every leaf of the given datasource is a {@link TableDataSource}.
   */
  private static boolean readsOnlyTables(final DataSource dataSource)
  {
    final List<DataSource> children = dataSource.getChildren();

    if (children.isEmpty()) {
      return dataSource instanceof TableDataSource;
    } else {
      return children.stream().allMatch(UnionDataSource::readsOnlyTables);
    }
  }
}
