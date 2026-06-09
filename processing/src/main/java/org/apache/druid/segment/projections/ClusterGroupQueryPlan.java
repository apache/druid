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
import org.apache.druid.segment.filter.FalseFilter;
import org.apache.druid.segment.filter.TrueFilter;

import javax.annotation.Nullable;
import java.util.List;
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

  ClusterGroupQueryPlan(
      List<TableClusterGroupSpec> survivingGroups,
      Function<TableClusterGroupSpec, Filter> rewriter
  )
  {
    this.survivingGroups = survivingGroups;
    this.rewriter = rewriter;
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
}
