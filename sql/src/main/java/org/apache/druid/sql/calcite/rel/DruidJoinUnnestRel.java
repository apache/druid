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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;
import java.util.Set;


public class DruidJoinUnnestRel extends DruidRel<DruidJoinUnnestRel>
{
  private static final TableDataSource DUMMY_DATA_SOURCE = new TableDataSource("__correlate_unnest__");

  public DruidUnnestRel getUnnestRel()
  {
    return unnestRel;
  }

  public DruidRel<?> getRightRel()
  {
    return rightRel;
  }

  public Join getJoin()
  {
    return join;
  }

  private final DruidUnnestRel unnestRel;
  private final DruidRel<?> rightRel;

  private final PartialDruidQuery partialDruidQuery;

  private final Join join;

  protected DruidJoinUnnestRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      PlannerContext plannerContext,
      Join joinRel,
      DruidRel<?> left,
      DruidRel<?> right,
      PartialDruidQuery pq
  )
  {
    super(cluster, traitSet, plannerContext);
    this.unnestRel = (DruidUnnestRel) left;
    this.rightRel = right;
    this.join = joinRel;
    this.partialDruidQuery = pq;
  }

  public static DruidJoinUnnestRel create(Join join, DruidRel<?> left, DruidRel<?> right, PlannerContext context)
  {
    return new DruidJoinUnnestRel(
        join.getCluster(),
        join.getTraitSet(),
        context,
        join,
        left,
        right,
        PartialDruidQuery.create(join)
    );
  }

  @Nullable
  @Override
  public PartialDruidQuery getPartialDruidQuery()
  {
    return partialDruidQuery;
  }

  @Override
  public DruidJoinUnnestRel withPartialQuery(PartialDruidQuery newQueryBuilder)
  {
    return new DruidJoinUnnestRel(
        getCluster(),
        newQueryBuilder.getTraitSet(getConvention()),
        getPlannerContext(),
        join,
        unnestRel,
        rightRel,
        newQueryBuilder
    );
  }

  @Override
  public DruidQuery toDruidQuery(boolean finalizeAggregations)
  {
    throw new CannotBuildQueryException("Cannot execute JOIN with just an UNNEST on left directly");
  }

  @Override
  public DruidQuery toDruidQueryForExplaining()
  {
    return partialDruidQuery.build(
        DUMMY_DATA_SOURCE,
        RowSignatures.fromRelDataType(
            join.getRowType().getFieldNames(),
            join.getRowType()
        ),
        getPlannerContext(),
        getCluster().getRexBuilder(),
        false
    );
  }

  @Override
  protected RelDataType deriveRowType()
  {
    return partialDruidQuery.getRowType();
  }

  @Override
  public RelWriter explainTerms(RelWriter pw)
  {
    return pw.item("join", join).item("left", unnestRel).item("right", rightRel);
  }

  @Override
  public DruidJoinUnnestRel asDruidConvention()
  {
    return new DruidJoinUnnestRel(
        getCluster(),
        getTraitSet(),
        getPlannerContext(),
        join,
        unnestRel,
        rightRel,
        partialDruidQuery
    );
  }

  @Override
  public Set<String> getDataSourceNames()
  {
    // left is only unnest which will be re-written outside
    return rightRel.getDataSourceNames();
  }
}
