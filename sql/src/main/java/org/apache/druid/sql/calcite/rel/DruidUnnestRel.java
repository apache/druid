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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Set;

/**
 * Captures an unnest expression for a correlated join. Derived from an {@link Uncollect}.
 *
 * This rel cannot be executed directly. It is a holder of information for {@link DruidCorrelateUnnestRel}.
 *
 * Unnest on literal values, without correlated join, is handled directly by
 * {@link org.apache.druid.sql.calcite.rule.DruidUnnestRule}. is applied without a correlated join, This covers the case where an unnest has an
 * input table, this rel resolves the unnest part and delegates the rel to be consumed by other
 * rule ({@link org.apache.druid.sql.calcite.rule.DruidCorrelateUnnestRule}
 */
public class DruidUnnestRel extends DruidRel<DruidUnnestRel>
{
  private static final String FIELD_NAME = "UNNEST";

  /**
   * Expression to be unnested. May be constant or may reference a correlation variable through a
   * {@link org.apache.calcite.rex.RexFieldAccess}.
   */
  private final RexNode inputRexNode;
  private final Filter unnestFilter;

  private DruidUnnestRel(
      final RelOptCluster cluster,
      final RelTraitSet traits,
      final RexNode inputRexNode,
      final Filter unnestFilter,
      final PlannerContext plannerContext
  )
  {
    super(cluster, traits, plannerContext);
    this.inputRexNode = inputRexNode;
    this.unnestFilter = unnestFilter;
  }

  public Filter getUnnestFilter()
  {
    return unnestFilter;
  }

  public static DruidUnnestRel create(
      final RelOptCluster cluster,
      final RelTraitSet traits,
      final RexNode unnestRexNode,
      final PlannerContext plannerContext
  )
  {
    if (!RelOptUtil.InputFinder.bits(unnestRexNode).isEmpty()) {
      throw new ISE("Expression must not include field references");
    }

    return new DruidUnnestRel(
        cluster,
        traits,
        unnestRexNode,
        null,
        plannerContext
    );
  }

  @Override
  @SuppressWarnings("ObjectEquality")
  public RelNode accept(RexShuttle shuttle)
  {
    final RexNode newInputRexNode = shuttle.apply(inputRexNode);

    if (newInputRexNode == inputRexNode) {
      return this;
    } else {
      return new DruidUnnestRel(
          getCluster(),
          getTraitSet(),
          newInputRexNode,
          unnestFilter,
          getPlannerContext()
      );
    }
  }

  /**
   * Expression to be unnested.
   */
  public RexNode getInputRexNode()
  {
    return inputRexNode;
  }

  @Override
  public PartialDruidQuery getPartialDruidQuery()
  {
    return null;
  }

  @Override
  public DruidUnnestRel withPartialQuery(PartialDruidQuery newQueryBuilder)
  {
    throw new UnsupportedOperationException();
  }

  public DruidUnnestRel withFilter(Filter f)
  {
    return new DruidUnnestRel(getCluster(), getTraitSet(), inputRexNode, f, getPlannerContext());
  }

  /**
   * Returns a new rel with a new input. The output type is unchanged.
   */
  public DruidUnnestRel withUnnestRexNode(final RexNode newInputRexNode)
  {
    return new DruidUnnestRel(
        getCluster(),
        getTraitSet(),
        newInputRexNode,
        unnestFilter,
        getPlannerContext()
    );
  }

  @Override
  public DruidQuery toDruidQuery(boolean finalizeAggregations)
  {
    // DruidUnnestRel is a holder for info for DruidCorrelateUnnestRel. It cannot be executed on its own.
    throw new CannotBuildQueryException("Cannot execute UNNEST directly");
  }

  @Override
  public DruidQuery toDruidQueryForExplaining()
  {
    // DruidUnnestRel is a holder for info for DruidCorrelateUnnestRel. It cannot be executed on its own.
    throw new CannotBuildQueryException("Cannot execute UNNEST directly");
  }

  @Nullable
  @Override
  public DruidUnnestRel asDruidConvention()
  {
    return new DruidUnnestRel(
        getCluster(),
        getTraitSet().replace(DruidConvention.instance()),
        inputRexNode,
        unnestFilter,
        getPlannerContext()
    );
  }

  @Override
  public RelWriter explainTerms(RelWriter pw)
  {
    return pw.item("expr", inputRexNode).item("filter", unnestFilter);
  }

  @Override
  public Set<String> getDataSourceNames()
  {
    return Collections.emptySet();
  }

  @Override
  protected RelDataType deriveRowType()
  {
    return Uncollect.deriveUncollectRowType(
        LogicalValues.createEmpty(
            getCluster(),
            RexUtil.createStructType(
                getCluster().getTypeFactory(),
                ImmutableList.of(inputRexNode),
                null,
                SqlValidatorUtil.F_SUGGESTER
            )
        ),
        false,
        Collections.emptyList()
    );
  }
}
