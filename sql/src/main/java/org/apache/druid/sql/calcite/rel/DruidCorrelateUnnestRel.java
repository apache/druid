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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This is the DruidRel to handle correlated rel nodes to be used for unnest.
 * Each correlate can be perceived as a join with the join type being inner
 * the left of a correlate as seen in the rule {@link org.apache.druid.sql.calcite.rule.DruidCorrelateUnnestRule}
 * is the {@link DruidQueryRel} while the right will always be an {@link DruidUnnestDatasourceRel}.
 *
 * Since this is a subclass of DruidRel it is automatically considered by other rules that involves DruidRels.
 * Some example being SELECT_PROJECT and SORT_PROJECT rules in {@link org.apache.druid.sql.calcite.rule.DruidRules.DruidQueryRule}
 */
public class DruidCorrelateUnnestRel extends DruidRel<DruidCorrelateUnnestRel>
{
  private static final TableDataSource DUMMY_DATA_SOURCE = new TableDataSource("unnest");

  private final Filter leftFilter;
  private final PartialDruidQuery partialQuery;
  private final PlannerConfig plannerConfig;
  private final Correlate correlateRel;
  private RelNode left;
  private RelNode right;

  private DruidCorrelateUnnestRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      Correlate correlateRel,
      PartialDruidQuery partialQuery,
      Filter baseFilter,
      PlannerContext plannerContext
  )
  {
    super(cluster, traitSet, plannerContext);
    this.correlateRel = correlateRel;
    this.partialQuery = partialQuery;
    this.left = correlateRel.getLeft();
    this.right = correlateRel.getRight();
    this.leftFilter = baseFilter;
    this.plannerConfig = plannerContext.getPlannerConfig();
  }

  /**
   * Create an instance from a Correlate that is based on a {@link DruidRel} and a {@link DruidUnnestDatasourceRel} inputs.
   */
  public static DruidCorrelateUnnestRel create(
      final Correlate correlateRel,
      final Filter leftFilter,
      final PlannerContext plannerContext
  )
  {
    return new DruidCorrelateUnnestRel(
        correlateRel.getCluster(),
        correlateRel.getTraitSet(),
        correlateRel,
        PartialDruidQuery.create(correlateRel),
        leftFilter,
        plannerContext
    );
  }

  @Nullable
  @Override
  public PartialDruidQuery getPartialDruidQuery()
  {
    return partialQuery;
  }

  @Override
  public DruidCorrelateUnnestRel withPartialQuery(PartialDruidQuery newQueryBuilder)
  {
    return new DruidCorrelateUnnestRel(
        getCluster(),
        getTraitSet().plusAll(newQueryBuilder.getRelTraits()),
        correlateRel,
        newQueryBuilder,
        leftFilter,
        getPlannerContext()
    );
  }

  @Override
  public DruidQuery toDruidQuery(boolean finalizeAggregations)
  {
    final DruidRel<?> druidQueryRel = (DruidRel<?>) left;
    final DruidQuery leftQuery = Preconditions.checkNotNull((druidQueryRel).toDruidQuery(false), "leftQuery");
    final DataSource leftDataSource;

    if (DruidJoinQueryRel.computeLeftRequiresSubquery(druidQueryRel)) {
      leftDataSource = new QueryDataSource(leftQuery.getQuery());
    } else {
      leftDataSource = leftQuery.getDataSource();
    }

    final DruidUnnestDatasourceRel unnestDatasourceRel = (DruidUnnestDatasourceRel) right;


    final RowSignature rowSignature = RowSignatures.fromRelDataType(
        correlateRel.getRowType().getFieldNames(),
        correlateRel.getRowType()
    );

    final DruidExpression expression = Expressions.toDruidExpression(
        getPlannerContext(),
        rowSignature,
        unnestDatasourceRel.getUnnestProject().getProjects().get(0)
    );

    LogicalProject unnestProject = LogicalProject.create(
        this,
        ImmutableList.of(unnestDatasourceRel.getUnnestProject()
                                            .getProjects()
                                            .get(0)),
        unnestDatasourceRel.getUnnestProject().getRowType()
    );

    // placeholder for dimension or expression to be unnested
    final String dimOrExpToUnnest;
    final VirtualColumnRegistry virtualColumnRegistry = VirtualColumnRegistry.create(
        rowSignature,
        getPlannerContext().getExprMacroTable(),
        getPlannerContext().getPlannerConfig().isForceExpressionVirtualColumns()
    );

    // the unnest project is needed in case of a virtual column
    // unnest(mv_to_array(dim_1)) is reconciled as unnesting a MVD dim_1 not requiring a virtual column
    // while unnest(array(dim_2,dim_3)) is understood as unnesting a virtual column which is an array over dim_2 and dim_3 elements
    boolean unnestProjectNeeded = false;
    getPlannerContext().setJoinExpressionVirtualColumnRegistry(virtualColumnRegistry);

    // handling for case when mv_to_array is used
    // No need to use virtual column in such a case
    if (StringUtils.toLowerCase(expression.getExpression()).startsWith("mv_to_array")) {
      dimOrExpToUnnest = expression.getArguments().get(0).getSimpleExtraction().getColumn();
    } else {
      if (expression.isDirectColumnAccess()) {
        dimOrExpToUnnest = expression.getDirectColumn();
      } else {
        // buckle up time to create virtual columns on expressions
        unnestProjectNeeded = true;
        dimOrExpToUnnest = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
            expression,
            expression.getDruidType()
        );
      }
    }

    // add the unnest project to the partial query if required
    // This is necessary to handle the virtual columns on the unnestProject
    // Also create the unnest datasource to be used by the partial query
    PartialDruidQuery partialDruidQuery = unnestProjectNeeded ? partialQuery.withUnnest(unnestProject) : partialQuery;
    return partialDruidQuery.build(
        UnnestDataSource.create(
            leftDataSource,
            dimOrExpToUnnest,
            unnestDatasourceRel.getUnnestProject().getRowType().getFieldNames().get(0),
            null
        ),
        rowSignature,
        getPlannerContext(),
        getCluster().getRexBuilder(),
        finalizeAggregations,
        virtualColumnRegistry
    );
  }

  @Override
  protected DruidCorrelateUnnestRel clone() throws CloneNotSupportedException
  {
    return DruidCorrelateUnnestRel.create(correlateRel, leftFilter, getPlannerContext());
  }

  @Override
  protected RelDataType deriveRowType()
  {
    return partialQuery.getRowType();
  }

  @Override
  public DruidQuery toDruidQueryForExplaining()
  {
    return partialQuery.build(
        DUMMY_DATA_SOURCE,
        RowSignatures.fromRelDataType(
            correlateRel.getRowType().getFieldNames(),
            correlateRel.getRowType()
        ),
        getPlannerContext(),
        getCluster().getRexBuilder(),
        false
    );
  }

  // This is required to be overwritten as Calcite uses this method
  // to maintain a map of equivalent DruidCorrelateUnnestRel or in general any Rel nodes.
  // Without this method overwritten multiple RelNodes will produce the same key
  // which makes the planner plan incorrectly.
  @Override
  public RelWriter explainTerms(RelWriter pw)
  {
    final String queryString;
    final DruidQuery druidQuery = toDruidQueryForExplaining();

    try {
      queryString = getPlannerContext().getJsonMapper().writeValueAsString(druidQuery.getQuery());
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    return pw.item("query", queryString)
             .item("signature", druidQuery.getOutputRowSignature());
  }

  // This is called from the DruidRelToDruidRule which converts from the NONE convention to the DRUID convention
  @Override
  public DruidCorrelateUnnestRel asDruidConvention()
  {
    return new DruidCorrelateUnnestRel(
        getCluster(),
        getTraitSet().replace(DruidConvention.instance()),
        correlateRel.copy(
            correlateRel.getTraitSet(),
            correlateRel.getInputs()
                        .stream()
                        .map(input -> RelOptRule.convert(input, DruidConvention.instance()))
                        .collect(Collectors.toList())
        ),
        partialQuery,
        leftFilter,
        getPlannerContext()
    );
  }

  @Override
  public List<RelNode> getInputs()
  {
    return ImmutableList.of(left, right);
  }

  @Override
  public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs)
  {
    return new DruidCorrelateUnnestRel(
        getCluster(),
        traitSet,
        correlateRel.copy(correlateRel.getTraitSet(), inputs),
        getPartialDruidQuery(),
        leftFilter,
        getPlannerContext()
    );
  }

  @Override
  public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq)
  {
    double cost;

    if (DruidJoinQueryRel.computeLeftRequiresSubquery(DruidJoinQueryRel.getSomeDruidChild(left))) {
      cost = CostEstimates.COST_SUBQUERY;
    } else {
      cost = partialQuery.estimateCost();
      if (correlateRel.getJoinType() == JoinRelType.INNER && plannerConfig.isComputeInnerJoinCostAsFilter()) {
        cost *= CostEstimates.MULTIPLIER_FILTER;
      }
    }

    return planner.getCostFactory().makeCost(cost, 0, 0);
  }

  @Override
  public Set<String> getDataSourceNames()
  {
    final Set<String> retVal = new HashSet<>();
    retVal.addAll(((DruidRel<?>) left).getDataSourceNames());
    retVal.addAll(((DruidRel<?>) right).getDataSourceNames());
    return retVal;
  }
}
