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

package org.apache.druid.sql.calcite.rel;

import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.server.QueryResponse;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents a "UNION ALL" of various input {@link DruidRel}. Note that this rel doesn't represent a real native query,
 * but rather, it represents the concatenation of a series of native queries in the SQL layer. Therefore,
 * {@link #getPartialDruidQuery()} returns null, and this rel cannot be built on top of. It must be the outer rel in a
 * query plan.
 * <p>
 * See {@link DruidUnionDataSourceRel} for a version that does a regular Druid query using a {@link UnionDataSource}.
 * In the future we expect that {@link UnionDataSource} will gain the ability to union query datasources together, and
 * then this rel could be replaced by {@link DruidUnionDataSourceRel}.
 */
public class DruidUnionRel extends DruidRel<DruidUnionRel>
{
  private final RelDataType rowType;
  private final List<RelNode> rels;
  private final int limit;

  private DruidUnionRel(
      final RelOptCluster cluster,
      final RelTraitSet traitSet,
      final PlannerContext plannerContext,
      final RelDataType rowType,
      final List<RelNode> rels,
      final int limit
  )
  {
    super(cluster, traitSet, plannerContext);
    this.rowType = rowType;
    this.rels = rels;
    this.limit = limit;
  }

  public static DruidUnionRel create(
      final PlannerContext plannerContext,
      final RelDataType rowType,
      final List<RelNode> rels,
      final int limit
  )
  {
    Preconditions.checkState(rels.size() > 0, "rels must be nonempty");

    return new DruidUnionRel(
        rels.get(0).getCluster(),
        rels.get(0).getTraitSet(),
        plannerContext,
        rowType,
        new ArrayList<>(rels),
        limit
    );
  }

  @Override
  @Nullable
  public PartialDruidQuery getPartialDruidQuery()
  {
    return null;
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public QueryResponse<Object[]> runQuery()
  {
    // Lazy: run each query in sequence, not all at once.
    if (limit == 0) {
      return new QueryResponse<Object[]>(Sequences.empty(), ResponseContext.createEmpty());
    } else {

      // We run the first rel here for two reasons:
      // 1) So that we get things running as normally expected when runQuery() is called
      // 2) So that we have a QueryResponse to return, note that the response headers from the query will only
      //    have values from this first query and will not contain values from subsequent queries.  This is definitely
      //    sub-optimal, the other option would be to fire off all queries and combine their QueryResponses, but that
      //    is also sub-optimal as it would consume parallel query resources and potentially starve the system.
      //    Instead, we only return the headers from the first query and potentially exception out and fail the query
      //    if there are any response headers that come from subsequent queries that are correctness concerns
      final QueryResponse<Object[]> queryResponse = ((DruidRel) rels.get(0)).runQuery();

      final List<Sequence<Object[]>> firstAsList = Collections.singletonList(queryResponse.getResults());
      final Iterable<Sequence<Object[]>> theRestTransformed = FluentIterable
          .from(rels.subList(1, rels.size()))
          .transform(
              rel -> {
                final QueryResponse response = ((DruidRel) rel).runQuery();

                final ResponseContext nextContext = response.getResponseContext();
                final List<Interval> uncoveredIntervals = nextContext.getUncoveredIntervals();
                if (uncoveredIntervals == null || uncoveredIntervals.isEmpty()) {
                  return response.getResults();
                } else {
                  throw new ISE(
                      "uncoveredIntervals[%s] existed on a sub-query of a union, incomplete data, failing",
                      uncoveredIntervals
                  );
                }
              }
          );

      final Iterable<Sequence<Object[]>> recombinedSequences = Iterables.concat(firstAsList, theRestTransformed);

      final Sequence returnSequence = Sequences.concat(recombinedSequences);
      return new QueryResponse<Object[]>(
          limit > 0 ? returnSequence.limit(limit) : returnSequence,
          queryResponse.getResponseContext()
      );
    }
  }

  @Override
  public DruidUnionRel withPartialQuery(final PartialDruidQuery newQueryBuilder)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public DruidQuery toDruidQuery(final boolean finalizeAggregations)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public DruidQuery toDruidQueryForExplaining()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public DruidUnionRel asDruidConvention()
  {
    return new DruidUnionRel(
        getCluster(),
        getTraitSet().replace(DruidConvention.instance()),
        getPlannerContext(),
        rowType,
        rels.stream().map(rel -> RelOptRule.convert(rel, DruidConvention.instance())).collect(Collectors.toList()),
        limit
    );
  }

  @Override
  public List<RelNode> getInputs()
  {
    return rels;
  }

  @Override
  public void replaceInput(int ordinalInParent, RelNode p)
  {
    rels.set(ordinalInParent, p);
  }

  @Override
  public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs)
  {
    return new DruidUnionRel(
        getCluster(),
        traitSet,
        getPlannerContext(),
        rowType,
        inputs,
        limit
    );
  }

  @Override
  public Set<String> getDataSourceNames()
  {
    return rels.stream()
               .flatMap(rel -> ((DruidRel<?>) rel).getDataSourceNames().stream())
               .collect(Collectors.toSet());
  }

  @Override
  public RelWriter explainTerms(RelWriter pw)
  {
    super.explainTerms(pw);

    for (int i = 0; i < rels.size(); i++) {
      pw.input(StringUtils.format("input#%d", i), rels.get(i));
    }

    return pw.item("limit", limit);
  }

  @Override
  protected RelDataType deriveRowType()
  {
    return rowType;
  }

  @Override
  public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq)
  {
    return planner.getCostFactory().makeCost(CostEstimates.COST_BASE, 0, 0);
  }

  public int getLimit()
  {
    return limit;
  }
}
