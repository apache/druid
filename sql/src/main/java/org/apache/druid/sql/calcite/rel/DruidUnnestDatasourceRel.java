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

import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Set;

/**
 * The Rel node to capture the unnest (or uncollect) part in a query. This covers 2 cases:
 *
 * Case 1:
 * If this is an unnest on a constant and no input table is required, the final query is built using
 * an UnnestDataSource with a base InlineDataSource in this rel.
 *
 * Case 2:
 * If the unnest has an input table, this rel resolves the unnest part and delegates the rel to be consumed by other
 * rule ({@link org.apache.druid.sql.calcite.rule.DruidCorrelateUnnestRule}
 */
public class DruidUnnestDatasourceRel extends DruidRel<DruidUnnestDatasourceRel>
{
  private final Uncollect uncollect;
  private final DruidQueryRel druidQueryRel;
  private final LogicalProject unnestProject;

  public DruidUnnestDatasourceRel(
      Uncollect uncollect,
      DruidQueryRel queryRel,
      LogicalProject unnestProject,
      PlannerContext plannerContext
  )
  {
    super(uncollect.getCluster(), uncollect.getTraitSet(), plannerContext);
    this.uncollect = uncollect;
    this.druidQueryRel = queryRel;
    this.unnestProject = unnestProject;
  }

  public LogicalProject getUnnestProject()
  {
    return unnestProject;
  }

  @Nullable
  @Override
  public PartialDruidQuery getPartialDruidQuery()
  {
    return druidQueryRel.getPartialDruidQuery();
  }

  @Override
  public DruidUnnestDatasourceRel withPartialQuery(PartialDruidQuery newQueryBuilder)
  {
    return new DruidUnnestDatasourceRel(
        uncollect,
        druidQueryRel.withPartialQuery(newQueryBuilder),
        unnestProject,
        getPlannerContext()
    );
  }

  @Override
  public DruidQuery toDruidQuery(boolean finalizeAggregations)
  {
    VirtualColumnRegistry virtualColumnRegistry = VirtualColumnRegistry.create(
        druidQueryRel.getDruidTable().getRowSignature(),
        getPlannerContext().getExprMacroTable(),
        getPlannerContext().getPlannerConfig().isForceExpressionVirtualColumns()
    );
    getPlannerContext().setJoinExpressionVirtualColumnRegistry(virtualColumnRegistry);

    final DruidExpression expression = Expressions.toDruidExpression(
        getPlannerContext(),
        druidQueryRel.getDruidTable().getRowSignature(),
        unnestProject.getProjects().get(0)
    );
    if (expression == null) {
      return null;
    }
    Expr parsed = expression.parse(getPlannerContext().getExprMacroTable());
    ExprEval eval = parsed.eval(InputBindings.nilBindings());

    // If query unnests a constant expression and not use any table
    // the unnest would be on an inline data source
    // with the input column being called "inline" in the native query
    UnnestDataSource dataSource = UnnestDataSource.create(
        InlineDataSource.fromIterable(
            Collections.singletonList(new Object[]{eval.value()}),
            RowSignature.builder().add("inline", ExpressionType.toColumnType(eval.type())).build()
        ),
        "inline",
        druidQueryRel.getRowType().getFieldNames().get(0),
        null
    );

    DruidQuery query = druidQueryRel.getPartialDruidQuery().build(
        dataSource,
        RowSignatures.fromRelDataType(uncollect.getRowType().getFieldNames(), uncollect.getRowType()),
        getPlannerContext(),
        getCluster().getRexBuilder(),
        finalizeAggregations
    );
    getPlannerContext().setJoinExpressionVirtualColumnRegistry(null);
    return query;
  }

  @Override
  public DruidQuery toDruidQueryForExplaining()
  {
    return toDruidQuery(false);
  }

  @Override
  public DruidUnnestDatasourceRel asDruidConvention()
  {
    return new DruidUnnestDatasourceRel(
        new Uncollect(getCluster(), traitSet.replace(DruidConvention.instance()), uncollect.getInput(), false),
        druidQueryRel.asDruidConvention(),
        unnestProject,
        getPlannerContext()
    );
  }

  @Override
  public RelWriter explainTerms(RelWriter pw)
  {
    return super.explainTerms(pw);
  }

  @Override
  public Set<String> getDataSourceNames()
  {
    return druidQueryRel.getDruidTable().getDataSource().getTableNames();
  }

  @Override
  protected RelDataType deriveRowType()
  {
    return uncollect.getRowType();
  }

  @Override
  protected DruidUnnestDatasourceRel clone() throws CloneNotSupportedException
  {
    return new DruidUnnestDatasourceRel(uncollect, druidQueryRel, unnestProject, getPlannerContext());
  }
}
