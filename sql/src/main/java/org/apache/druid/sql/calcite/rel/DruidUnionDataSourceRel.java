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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.table.RowSignatures;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents a query on top of a {@link UnionDataSource}. This is used to represent a "UNION ALL" of regular table
 * datasources.
 *
 * See {@link DruidUnionRel} for a version that can union any set of queries together (not just regular tables),
 * but also must be the outermost rel of a query plan. In the future we expect that {@link UnionDataSource} will gain
 * the ability to union query datasources together, and then this class could replace {@link DruidUnionRel}.
 */
public class DruidUnionDataSourceRel extends DruidRel<DruidUnionDataSourceRel>
{
  private static final TableDataSource DUMMY_DATA_SOURCE = new TableDataSource("__union__");

  private final Union unionRel;
  private final List<String> unionColumnNames;
  private final PartialDruidQuery partialQuery;

  private DruidUnionDataSourceRel(
      final RelOptCluster cluster,
      final RelTraitSet traitSet,
      final Union unionRel,
      final List<String> unionColumnNames,
      final PartialDruidQuery partialQuery,
      final QueryMaker queryMaker
  )
  {
    super(cluster, traitSet, queryMaker);
    this.unionRel = unionRel;
    this.unionColumnNames = unionColumnNames;
    this.partialQuery = partialQuery;
  }

  public static DruidUnionDataSourceRel create(
      final Union unionRel,
      final List<String> unionColumnNames,
      final QueryMaker queryMaker
  )
  {
    return new DruidUnionDataSourceRel(
        unionRel.getCluster(),
        unionRel.getTraitSet(),
        unionRel,
        unionColumnNames,
        PartialDruidQuery.create(unionRel),
        queryMaker
    );
  }

  public List<String> getUnionColumnNames()
  {
    return unionColumnNames;
  }

  @Override
  public PartialDruidQuery getPartialDruidQuery()
  {
    return partialQuery;
  }

  @Override
  public DruidUnionDataSourceRel withPartialQuery(final PartialDruidQuery newQueryBuilder)
  {
    return new DruidUnionDataSourceRel(
        getCluster(),
        getTraitSet().plusAll(newQueryBuilder.getRelTraits()),
        unionRel,
        unionColumnNames,
        newQueryBuilder,
        getQueryMaker()
    );
  }

  @Override
  public Sequence<Object[]> runQuery()
  {
    // runQuery doesn't need to finalize aggregations, because the fact that runQuery is happening suggests this
    // is the outermost query and it will actually get run as a native query. Druid's native query layer will
    // finalize aggregations for the outermost query even if we don't explicitly ask it to.

    return getQueryMaker().runQuery(toDruidQuery(false));
  }

  @Override
  public DruidQuery toDruidQuery(final boolean finalizeAggregations)
  {
    final List<TableDataSource> dataSources = new ArrayList<>();
    RowSignature signature = null;

    for (final RelNode relNode : unionRel.getInputs()) {
      final DruidRel<?> druidRel = (DruidRel<?>) relNode;
      if (!DruidRels.isScanOrMapping(druidRel, false)) {
        throw new CannotBuildQueryException(druidRel);
      }

      final DruidQuery query = druidRel.toDruidQuery(false);
      final DataSource dataSource = query.getDataSource();
      if (!(dataSource instanceof TableDataSource)) {
        throw new CannotBuildQueryException(druidRel);
      }

      if (signature == null) {
        signature = query.getOutputRowSignature();
      }

      if (signature.getColumnNames().equals(query.getOutputRowSignature().getColumnNames())) {
        dataSources.add((TableDataSource) dataSource);
      } else {
        throw new CannotBuildQueryException(druidRel);
      }
    }

    if (signature == null) {
      // No inputs.
      throw new CannotBuildQueryException(unionRel);
    }

    // Sanity check: the columns we think we're building off must equal the "unionColumnNames" registered at
    // creation time.
    if (!signature.getColumnNames().equals(unionColumnNames)) {
      throw new CannotBuildQueryException(unionRel);
    }

    return partialQuery.build(
        new UnionDataSource(dataSources),
        signature,
        getPlannerContext(),
        getCluster().getRexBuilder(),
        finalizeAggregations
    );
  }

  @Override
  public DruidQuery toDruidQueryForExplaining()
  {
    return partialQuery.build(
        DUMMY_DATA_SOURCE,
        RowSignatures.fromRelDataType(
            unionRel.getRowType().getFieldNames(),
            unionRel.getRowType()
        ),
        getPlannerContext(),
        getCluster().getRexBuilder(),
        false
    );
  }

  @Override
  public DruidUnionDataSourceRel asDruidConvention()
  {
    return new DruidUnionDataSourceRel(
        getCluster(),
        getTraitSet().replace(DruidConvention.instance()),
        (Union) unionRel.copy(
            unionRel.getTraitSet(),
            unionRel.getInputs()
                    .stream()
                    .map(input -> RelOptRule.convert(input, DruidConvention.instance()))
                    .collect(Collectors.toList())
        ),
        unionColumnNames,
        partialQuery,
        getQueryMaker()
    );
  }

  @Override
  public List<RelNode> getInputs()
  {
    return unionRel.getInputs();
  }

  @Override
  public void replaceInput(int ordinalInParent, RelNode p)
  {
    unionRel.replaceInput(ordinalInParent, p);
  }

  @Override
  public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs)
  {
    return new DruidUnionDataSourceRel(
        getCluster(),
        traitSet,
        (Union) unionRel.copy(unionRel.getTraitSet(), inputs),
        unionColumnNames,
        partialQuery,
        getQueryMaker()
    );
  }

  @Override
  public Set<String> getDataSourceNames()
  {
    final Set<String> retVal = new HashSet<>();

    for (final RelNode input : unionRel.getInputs()) {
      retVal.addAll(((DruidRel<?>) input).getDataSourceNames());
    }

    return retVal;
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

    for (int i = 0; i < unionRel.getInputs().size(); i++) {
      pw.input(StringUtils.format("input#%d", i), unionRel.getInputs().get(i));
    }

    return pw.item("query", queryString)
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
    return planner.getCostFactory().makeZeroCost();
  }
}
