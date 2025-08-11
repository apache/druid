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

package org.apache.druid.segment.projections;

import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * State holder used by {@link Projections#matchAggregateProjection} for building a {@link ProjectionMatch}. Tracks
 * which all kinds of stuff to map a projection to the contents of a query {@link CursorBuildSpec}, accumulating
 * details about how to transform into a new {@link CursorBuildSpec} that will run against the projection.
 */
public final class ProjectionMatchBuilder
{
  private final Set<String> referencedPhysicalColumns;
  private final Set<VirtualColumn> referencedVirtualColumns;
  private final Map<String, String> remapColumns;
  private final List<AggregatorFactory> combiningFactories;
  private final Set<String> matchedQueryColumns;
  @Nullable
  private Filter rewriteFilter;

  public ProjectionMatchBuilder()
  {
    this.referencedPhysicalColumns = new HashSet<>();
    this.referencedVirtualColumns = new HashSet<>();
    this.remapColumns = new HashMap<>();
    this.combiningFactories = new ArrayList<>();
    this.matchedQueryColumns = new HashSet<>();
  }

  /**
   * Map a query column name to a projection column name
   */
  public ProjectionMatchBuilder remapColumn(String queryColumn, String projectionColumn)
  {
    remapColumns.put(queryColumn, projectionColumn);
    return this;
  }

  @Nullable
  public String getRemapValue(String queryColumn)
  {
    return remapColumns.get(queryColumn);
  }

  /**
   * Add a projection physical column, which will later be added to {@link ProjectionMatch#getCursorBuildSpec()} if
   * the projection matches
   */
  public ProjectionMatchBuilder addReferencedPhysicalColumn(String column)
  {
    referencedPhysicalColumns.add(column);
    return this;
  }

  /**
   * Add a query virtual column that can use projection physical columns as inputs to the match builder, which will
   * later be added to {@link ProjectionMatch#getCursorBuildSpec()} if the projection matches
   */
  public ProjectionMatchBuilder addReferenceedVirtualColumn(VirtualColumn virtualColumn)
  {
    referencedVirtualColumns.add(virtualColumn);
    return this;
  }

  /**
   * Add a query {@link AggregatorFactory#substituteCombiningFactory(AggregatorFactory)} which can combine the inputs
   * of a selector created by a projection {@link AggregatorFactory}
   */
  public ProjectionMatchBuilder addPreAggregatedAggregator(AggregatorFactory aggregator)
  {
    combiningFactories.add(aggregator);
    return this;
  }

  public ProjectionMatchBuilder addMatchedQueryColumn(String queryColumn)
  {
    matchedQueryColumns.add(queryColumn);
    return this;
  }

  public ProjectionMatchBuilder addMatchedQueryColumns(Collection<String> queryColumns)
  {
    matchedQueryColumns.addAll(queryColumns);
    return this;
  }

  public ProjectionMatchBuilder rewriteFilter(Filter rewriteFilter)
  {
    this.rewriteFilter = rewriteFilter;
    return this;
  }

  public Filter getRewriteFilter()
  {
    return rewriteFilter;
  }

  public Map<String, String> getRemapColumns()
  {
    return remapColumns;
  }

  public Set<String> getMatchedQueryColumns()
  {
    return matchedQueryColumns;
  }

  public ProjectionMatch build(CursorBuildSpec queryCursorBuildSpec)
  {
    return new ProjectionMatch(
        CursorBuildSpec.builder(queryCursorBuildSpec)
                       .setFilter(rewriteFilter)
                       .setPhysicalColumns(referencedPhysicalColumns)
                       .setVirtualColumns(VirtualColumns.fromIterable(referencedVirtualColumns))
                       .setAggregators(combiningFactories)
                       .build(),
        remapColumns
    );
  }
}
