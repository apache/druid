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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Per-query plan produced by {@link Projections#planClusterGroupQuery} for a clustered base-table segment: the
 * subset of cluster groups whose clustering values can't rule the query filter out (the surviving groups), a per-group
 * filter rewrite that folds clustering-column leaves against each group's constant clustering tuple (see
 * {@link #rewriteFor}), and a query-level {@link #virtualColumnRemap()} of query virtual columns equivalent to a
 * materialized column. Production callers (the cursor factories) iterate {@link #survivingGroups()} and call
 * {@link #rebuildCursorBuildSpec} once per surviving group to produce that group's {@link CursorBuildSpec}.
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
   * Per-group rewrite of the query's filter against {@code group}'s constant clustering tuple: recognized
   * equality / in / null leaves on a clustering column collapse to {@link TrueFilter} / {@link FalseFilter} and fold
   * through AND / OR / NOT. Non-clustering leaves and other filter shapes on a clustering column (e.g. range / like)
   * are left as-is for the per-group cursor to evaluate per-row; the cursor exposes clustering columns as per-group
   * constants, so a surviving clustering-column leaf still resolves rather than reading a missing column.
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
   * columns, and the per-group filter's required columns are rewritten via the remap so any surviving leaf that
   * referenced a dropped VC now points at its materialized column: a non-clustering VC maps to the stored physical
   * column, a clustering-equivalent VC to the clustering column (the per-group cursor exposes it as a constant).
   * Foldable clustering-VC leaves (equality / in / null) were already collapsed to TRUE / FALSE by the per-group
   * rewrite walk and never reach the rewrite. Grouping / select / aggregator reads of a dropped VC are served by the
   * {@link org.apache.druid.segment.RemapColumnSelectorFactory} the cursor factory wraps around the per-group selector
   * factory (which exposes clustering columns as constants, whether from the fabricated per-group index or the
   * {@link ClusteringColumnSelectorFactory}).
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

    // Drop the remapped query virtual columns from the per-group spec; the materialized columns they map to are either
    // the group's constant clustering column or a stored per-group physical column. The cursor factory wraps the
    // per-group selector factory with a RemapColumnSelectorFactory so reads of the original query VC names resolve to
    // those materialized columns.
    final List<VirtualColumn> prunedVcs = new ArrayList<>();
    for (VirtualColumn vc : spec.getVirtualColumns().getVirtualColumns()) {
      if (!virtualColumnRemap.containsKey(vc.getOutputName())) {
        prunedVcs.add(vc);
      }
    }

    // Per-group filter rewrite first (folds recognized clustering leaves), then remap any surviving leaf that still
    // references a dropped VC to its materialized column.
    Filter rewritten = spec.getFilter() == null ? null : rewriteFor(group);
    if (rewritten != null) {
      rewritten = Projections.rewriteFilterRequiredColumns(rewritten, virtualColumnRemap);
    }

    final CursorBuildSpec.CursorBuildSpecBuilder builder = CursorBuildSpec.builder(spec)
                                                                          .setFilter(rewritten)
                                                                          .setVirtualColumns(VirtualColumns.create(prunedVcs));

    // The dropped query VCs now read their materialized target columns, so add them
    final Set<String> physicalColumns = spec.getPhysicalColumns();
    if (physicalColumns != null) {
      final Set<String> withTargets = new HashSet<>(physicalColumns);
      withTargets.addAll(virtualColumnRemap.values());
      builder.setPhysicalColumns(withTargets);
    }

    return builder.build();
  }
}

