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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class DruidUnionRel extends DruidRel<DruidUnionRel>
{
  private final RelDataType rowType;
  private final List<RelNode> rels;
  private final int limit;

  private DruidUnionRel(
      final RelOptCluster cluster,
      final RelTraitSet traitSet,
      final QueryMaker queryMaker,
      final RelDataType rowType,
      final List<RelNode> rels,
      final int limit
  )
  {
    super(cluster, traitSet, queryMaker);
    this.rowType = rowType;
    this.rels = rels;
    this.limit = limit;
  }

  public static DruidUnionRel create(
      final QueryMaker queryMaker,
      final RelDataType rowType,
      final List<RelNode> rels,
      final int limit
  )
  {
    Preconditions.checkState(rels.size() > 0, "rels must be nonempty");

    return new DruidUnionRel(
        rels.get(0).getCluster(),
        rels.get(0).getTraitSet(),
        queryMaker,
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
  @SuppressWarnings("unchecked")
  public Sequence<Object[]> runQuery()
  {
    // Lazy: run each query in sequence, not all at once.
    if (limit == 0) {
      return Sequences.empty();
    } else {
      final Sequence baseSequence = Sequences.concat(
          FluentIterable.from(rels).transform(rel -> ((DruidRel) rel).runQuery())
      );

      return limit > 0 ? baseSequence.limit(limit) : baseSequence;
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
        getQueryMaker(),
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
        getQueryMaker(),
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
