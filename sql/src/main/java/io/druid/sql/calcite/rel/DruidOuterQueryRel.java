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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.QueryDataSource;
import io.druid.query.TableDataSource;
import io.druid.query.filter.DimFilter;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

/**
 * DruidRel that uses a "query" dataSource.
 */
public class DruidOuterQueryRel extends DruidRel<DruidOuterQueryRel>
{
  private final RelNode sourceRel;
  private final DruidQueryBuilder queryBuilder;

  private DruidOuterQueryRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode sourceRel,
      DruidQueryBuilder queryBuilder,
      QueryMaker queryMaker
  )
  {
    super(cluster, traitSet, queryMaker);
    this.sourceRel = sourceRel;
    this.queryBuilder = queryBuilder;
  }

  public static DruidOuterQueryRel from(
      final DruidRel sourceRel,
      final DimFilter filter,
      final Grouping grouping,
      final RelDataType rowType,
      final List<String> rowOrder
  )
  {
    return new DruidOuterQueryRel(
        sourceRel.getCluster(),
        sourceRel.getTraitSet(),
        sourceRel,
        DruidQueryBuilder.fullScan(
            sourceRel.getOutputRowSignature(),
            sourceRel.getCluster().getTypeFactory()
        ).withFilter(filter).withGrouping(grouping, rowType, rowOrder),
        sourceRel.getQueryMaker()
    );
  }

  @Override
  public RowSignature getSourceRowSignature()
  {
    return ((DruidRel) sourceRel).getOutputRowSignature();
  }

  @Override
  public DruidQueryBuilder getQueryBuilder()
  {
    return queryBuilder;
  }

  @Override
  public Sequence<Object[]> runQuery()
  {
    final QueryDataSource queryDataSource = ((DruidRel) sourceRel).asDataSource();
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
  public DruidOuterQueryRel withQueryBuilder(final DruidQueryBuilder newQueryBuilder)
  {
    return new DruidOuterQueryRel(
        getCluster(),
        getTraitSet().plusAll(newQueryBuilder.getRelTraits()),
        sourceRel,
        newQueryBuilder,
        getQueryMaker()
    );
  }

  @Override
  public int getQueryCount()
  {
    return 1 + ((DruidRel) sourceRel).getQueryCount();
  }

  @Nullable
  @Override
  public QueryDataSource asDataSource()
  {
    final QueryDataSource queryDataSource = ((DruidRel) sourceRel).asDataSource();
    if (queryDataSource == null) {
      return null;
    } else {
      return new QueryDataSource(queryBuilder.toGroupByQuery(queryDataSource, getPlannerContext()));
    }
  }

  @Override
  public DruidOuterQueryRel asBindable()
  {
    return new DruidOuterQueryRel(
        getCluster(),
        getTraitSet().plus(BindableConvention.INSTANCE),
        sourceRel,
        queryBuilder,
        getQueryMaker()
    );
  }

  @Override
  public DruidOuterQueryRel asDruidConvention()
  {
    return new DruidOuterQueryRel(
        getCluster(),
        getTraitSet().plus(DruidConvention.instance()),
        RelOptRule.convert(sourceRel, DruidConvention.instance()),
        queryBuilder,
        getQueryMaker()
    );
  }

  @Override
  public List<RelNode> getInputs()
  {
    return ImmutableList.of(sourceRel);
  }

  @Override
  public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs)
  {
    return new DruidOuterQueryRel(
        getCluster(),
        traitSet,
        Iterables.getOnlyElement(inputs),
        getQueryBuilder(),
        getQueryMaker()
    );
  }

  @Override
  public RelWriter explainTerms(RelWriter pw)
  {
    final TableDataSource dummyDataSource = new TableDataSource("__subquery__");
    final String queryString;

    try {
      queryString = getQueryMaker()
          .getJsonMapper()
          .writeValueAsString(queryBuilder.toGroupByQuery(dummyDataSource, getPlannerContext()));
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    return super.explainTerms(pw)
                .input("innerQuery", sourceRel)
                .item("query", queryString);
  }

  @Override
  protected RelDataType deriveRowType()
  {
    return queryBuilder.getRowType();
  }

  @Override
  public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq)
  {
    return planner.getCostFactory().makeCost(mq.getRowCount(sourceRel), 0, 0);
  }
}
