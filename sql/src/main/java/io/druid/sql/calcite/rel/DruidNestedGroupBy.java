/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.rel;

import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.QueryDataSource;
import io.druid.query.filter.DimFilter;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

public class DruidNestedGroupBy extends DruidRel<DruidNestedGroupBy>
{
  private final DruidRel sourceRel;
  private final DruidQueryBuilder queryBuilder;

  private DruidNestedGroupBy(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      DruidRel sourceRel,
      DruidQueryBuilder queryBuilder
  )
  {
    super(cluster, traitSet, sourceRel.getQueryMaker());
    this.sourceRel = sourceRel;
    this.queryBuilder = queryBuilder;

    if (sourceRel.getQueryBuilder().getGrouping() == null) {
      throw new IllegalArgumentException("inner query must be groupBy");
    }

    if (queryBuilder.getGrouping() == null) {
      throw new IllegalArgumentException("outer query must be groupBy");
    }
  }

  public static DruidNestedGroupBy from(
      final DruidRel sourceRel,
      final DimFilter filter,
      final Grouping grouping,
      final RelDataType rowType,
      final List<String> rowOrder
  )
  {
    return new DruidNestedGroupBy(
        sourceRel.getCluster(),
        sourceRel.getTraitSet(),
        sourceRel,
        DruidQueryBuilder.fullScan(
            sourceRel.getOutputRowSignature(),
            sourceRel.getCluster().getTypeFactory()
        ).withFilter(filter).withGrouping(grouping, rowType, rowOrder)
    );
  }

  @Override
  public RowSignature getSourceRowSignature()
  {
    return sourceRel.getOutputRowSignature();
  }

  @Override
  public DruidQueryBuilder getQueryBuilder()
  {
    return queryBuilder;
  }

  @Override
  public Sequence<Object[]> runQuery()
  {
    final QueryDataSource queryDataSource = sourceRel.asDataSource();
    if (queryDataSource != null) {
      return getQueryMaker().runQuery(
          queryDataSource,
          queryBuilder
      );
    } else {
      return Sequences.empty();
    }
  }

  @Override
  public DruidNestedGroupBy withQueryBuilder(DruidQueryBuilder newQueryBuilder)
  {
    return new DruidNestedGroupBy(
        getCluster(),
        getTraitSet().plusAll(newQueryBuilder.getRelTraits()),
        sourceRel,
        newQueryBuilder
    );
  }

  @Override
  public int getQueryCount()
  {
    return 1 + sourceRel.getQueryCount();
  }

  @Override
  public QueryDataSource asDataSource()
  {
    final QueryDataSource queryDataSource = sourceRel.asDataSource();
    if (queryDataSource == null) {
      return null;
    } else {
      return new QueryDataSource(queryBuilder.toGroupByQuery(queryDataSource, getPlannerContext()));
    }
  }

  @Override
  public DruidNestedGroupBy asBindable()
  {
    return new DruidNestedGroupBy(
        getCluster(),
        getTraitSet().plus(BindableConvention.INSTANCE),
        sourceRel,
        queryBuilder
    );
  }

  @Override
  public DruidNestedGroupBy asDruidConvention()
  {
    return new DruidNestedGroupBy(
        getCluster(),
        getTraitSet().plus(DruidConvention.instance()),
        sourceRel,
        queryBuilder
    );
  }

  @Override
  public RelWriter explainTerms(RelWriter pw)
  {
    return pw
        .item("sourceRel", sourceRel)
        .item("queryBuilder", queryBuilder);
  }

  @Override
  protected RelDataType deriveRowType()
  {
    return queryBuilder.getRowType();
  }

  @Override
  public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq)
  {
    return sourceRel.computeSelfCost(planner, mq).multiplyBy(2.0);
  }
}
