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

import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.filter.FalseFilter;
import org.apache.druid.segment.filter.TrueFilter;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Per-query plan produced by {@link Projections#planClusterGroupQuery} for a clustered base-table segment: the
 * subset of cluster groups whose clustering values can't rule the query filter out (the surviving groups), plus a
 * per-group rewriter that produces the per-group cursor's filter by folding clustering-column leaves against each
 * group's constant clustering tuple. Production callers (the cursor factory) iterate {@link #survivingGroups()}
 * and call {@link #rewriteFor} once per surviving group.
 */
public final class ClusterGroupQueryPlan
{
  private final List<TableClusterGroupSpec> survivingGroups;
  private final Function<TableClusterGroupSpec, Filter> rewriter;
  private final Map<String, String> virtualColumnRemap;

  ClusterGroupQueryPlan(
      List<TableClusterGroupSpec> survivingGroups,
      Function<TableClusterGroupSpec, Filter> rewriter,
      Map<String, String> virtualColumnRemap
  )
  {
    this.survivingGroups = survivingGroups;
    this.rewriter = rewriter;
    this.virtualColumnRemap = virtualColumnRemap;
  }

  /**
   * The subset of input groups that this plan's query can't rule out from clustering values alone. Groups not in
   * this list are guaranteed not to match the query's filter and should be skipped at cursor build time.
   */
  public List<TableClusterGroupSpec> survivingGroups()
  {
    return survivingGroups;
  }

  /**
   * Query-level mapping of {@code queryVirtualColumnOutputName -> materializedColumnName} for query virtual columns
   * that are equivalent to a clustering column or a non-clustering materialized column in the clustered base table.
   * Empty when no query virtual column has a materialized equivalent (or there are no query virtual columns).
   */
  public Map<String, String> virtualColumnRemap()
  {
    return virtualColumnRemap;
  }

  /**
   * Per-group rewrite of the query's filter against {@code group}'s constant clustering tuple: clustering-column
   * leaves collapse to {@link TrueFilter} / {@link FalseFilter} and fold through AND / OR / NOT, so the rewritten
   * filter no longer references columns the per-group {@link org.apache.druid.segment.QueryableIndex} doesn't
   * physically carry. Non-clustering leaves and unrecognized filter types are left as-is.
   * <p>
   * Returns {@code null} when the original query filter was {@code null} (no rewrite needed), or
   * {@link FalseFilter} for a group that didn't survive pruning (would have been excluded from
   * {@link #survivingGroups()}).
   */
  @Nullable
  public Filter rewriteFor(TableClusterGroupSpec group)
  {
    return rewriter.apply(group);
  }

  /**
   * Rebuild {@code spec} for {@code group}'s per-group cursor by swapping in this plan's per-group filter rewrite
   * (see {@link #rewriteFor}) and, when {@link #virtualColumnRemap()} is non-empty, substituting any query virtual
   * columns that are equivalent to a materialized column.
   * <p>
   * When there is a remap, the matched query virtual columns (the remap keys) are dropped from the spec's virtual
   * columns, and the per-group filter's required columns are rewritten via the remap so that non-clustering-VC filter
   * leaves point at the materialized physical column (clustering-VC leaves were already folded to TRUE / FALSE by the
   * per-group rewrite walk, so they won't reference the dropped virtual columns). The grouping / select / aggregator
   * references are served instead by the {@link org.apache.druid.segment.RemapColumnSelectorFactory} the cursor
   * factory wraps around the {@link ClusteringColumnSelectorFactory}.
   */
  public CursorBuildSpec rebuildCursorBuildSpec(CursorBuildSpec spec, TableClusterGroupSpec group)
  {
    if (virtualColumnRemap.isEmpty()) {
      if (spec.getFilter() == null) {
        return spec;
      }
      final Filter rewritten = rewriteFor(group);
      if (rewritten == spec.getFilter()) {
        return spec;
      }
      return CursorBuildSpec.builder(spec).setFilter(rewritten).build();
    }

    // Drop the remapped query virtual columns from the per-group spec; the materialized columns they map to are
    // either the per-group constant clustering column or a per-group physical column, both served directly by the
    // ClusteringColumnSelectorFactory (the cursor factory additionally wraps it with a RemapColumnSelectorFactory so
    // the original query VC names resolve to the materialized columns).
    final List<VirtualColumn> prunedVcs = new ArrayList<>();
    for (VirtualColumn vc : spec.getVirtualColumns().getVirtualColumns()) {
      if (!virtualColumnRemap.containsKey(vc.getOutputName())) {
        prunedVcs.add(vc);
      }
    }

    // Per-group filter rewrite first (folds clustering leaves), then remap any surviving non-clustering-VC leaves to
    // the materialized physical column.
    Filter rewritten = spec.getFilter() == null ? null : rewriteFor(group);
    if (rewritten != null) {
      rewritten = rewritten.rewriteRequiredColumns(virtualColumnRemap);
    }

    return CursorBuildSpec.builder(spec)
                          .setFilter(rewritten)
                          .setVirtualColumns(VirtualColumns.create(prunedVcs))
                          .build();
  }
}

