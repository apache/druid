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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.QueryDataSource;
import io.druid.query.TableDataSource;
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
import java.util.List;

/**
 * DruidRel that uses a "query" dataSource.
 */
public class DruidOuterQueryRel extends DruidRel<DruidOuterQueryRel>
{
  private static final TableDataSource DUMMY_DATA_SOURCE = new TableDataSource("__subquery__");

  private final PartialDruidQuery partialQuery;
  private RelNode sourceRel;

  private DruidOuterQueryRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode sourceRel,
      PartialDruidQuery partialQuery,
      QueryMaker queryMaker
  )
  {
    super(cluster, traitSet, queryMaker);
    this.sourceRel = sourceRel;
    this.partialQuery = partialQuery;
  }

  public static DruidOuterQueryRel create(
      final DruidRel sourceRel,
      final PartialDruidQuery partialQuery
  )
  {
    return new DruidOuterQueryRel(
        sourceRel.getCluster(),
        sourceRel.getTraitSet(),
        sourceRel,
        partialQuery,
        sourceRel.getQueryMaker()
    );
  }

  @Override
  public PartialDruidQuery getPartialDruidQuery()
  {
    return partialQuery;
  }

  @Override
  public Sequence<Object[]> runQuery()
  {
    final DruidQuery query = toDruidQuery();
    if (query != null) {
      return getQueryMaker().runQuery(query);
    } else {
      return Sequences.empty();
    }
  }

  @Override
  public DruidOuterQueryRel withPartialQuery(final PartialDruidQuery newQueryBuilder)
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
  public DruidQuery toDruidQuery()
  {
    final DruidQuery subQuery = ((DruidRel) sourceRel).toDruidQuery();
    if (subQuery == null) {
      return null;
    }

    final RowSignature sourceRowSignature = subQuery.getOutputRowSignature();
    return partialQuery.build(
        new QueryDataSource(subQuery.toGroupByQuery()),
        sourceRowSignature,
        getPlannerContext(),
        getCluster().getRexBuilder()
    );
  }

  @Override
  public DruidQuery toDruidQueryForExplaining()
  {
    return partialQuery.build(
        DUMMY_DATA_SOURCE,
        RowSignature.from(
            sourceRel.getRowType().getFieldNames(),
            sourceRel.getRowType()
        ),
        getPlannerContext(),
        getCluster().getRexBuilder()
    );
  }

  @Override
  public DruidOuterQueryRel asBindable()
  {
    return new DruidOuterQueryRel(
        getCluster(),
        getTraitSet().plus(BindableConvention.INSTANCE),
        sourceRel,
        partialQuery,
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
        partialQuery,
        getQueryMaker()
    );
  }

  @Override
  public List<RelNode> getInputs()
  {
    return ImmutableList.of(sourceRel);
  }

  @Override
  public void replaceInput(int ordinalInParent, RelNode p)
  {
    if (ordinalInParent != 0) {
      throw new IndexOutOfBoundsException(StringUtils.format("Invalid ordinalInParent[%s]", ordinalInParent));
    }
    this.sourceRel = p;
  }

  @Override
  public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs)
  {
    return new DruidOuterQueryRel(
        getCluster(),
        traitSet,
        Iterables.getOnlyElement(inputs),
        getPartialDruidQuery(),
        getQueryMaker()
    );
  }

  @Override
  public List<String> getDatasourceNames()
  {
    return ((DruidRel) sourceRel).getDatasourceNames();
  }

  @Override
  public RelWriter explainTerms(RelWriter pw)
  {
    final String queryString;
    final DruidQuery druidQuery = toDruidQueryForExplaining();

    try {
      queryString = getQueryMaker().getJsonMapper().writeValueAsString(druidQuery.getQuery());
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    return super.explainTerms(pw)
                .input("innerQuery", sourceRel)
                .item("query", queryString)
                .item("signature", druidQuery.getOutputRowSignature());
  }

  @Override
  protected RelDataType deriveRowType()
  {
    return partialQuery.getRowType();
  }

  @Override
  public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq)
  {
    return planner.getCostFactory().makeCost(mq.getRowCount(sourceRel), 0, 0).multiplyBy(10);
  }
}
