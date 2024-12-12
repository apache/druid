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

package org.apache.druid.sql.calcite.rule;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.DruidOuterQueryRel;
import org.apache.druid.sql.calcite.rel.DruidRel;
import org.apache.druid.sql.calcite.rel.PartialDruidQuery;
import org.apache.druid.sql.calcite.run.EngineFeature;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Predicate;

public class DruidRules
{
  @SuppressWarnings("rawtypes")
  public static final Predicate<DruidRel> CAN_BUILD_ON = druidRel -> druidRel.getPartialDruidQuery() != null;

  private DruidRules()
  {
    // No instantiation.
  }

  public static List<RelOptRule> rules(PlannerContext plannerContext)
  {
    final ArrayList<RelOptRule> retVal = new ArrayList<>(
        ImmutableList.of(
            new DruidQueryRule<>(
                Filter.class,
                PartialDruidQuery.Stage.WHERE_FILTER,
                PartialDruidQuery::withWhereFilter
            ),
            new DruidQueryRule<>(
                Project.class,
                PartialDruidQuery.Stage.SELECT_PROJECT,
                PartialDruidQuery::withSelectProject
            ),
            new DruidQueryRule<>(
                Aggregate.class,
                PartialDruidQuery.Stage.AGGREGATE,
                PartialDruidQuery::withAggregate
            ),
            new DruidQueryRule<>(
                Project.class,
                PartialDruidQuery.Stage.AGGREGATE_PROJECT,
                PartialDruidQuery::withAggregateProject
            ),
            new DruidQueryRule<>(
                Filter.class,
                PartialDruidQuery.Stage.HAVING_FILTER,
                PartialDruidQuery::withHavingFilter
            ),
            new DruidQueryRule<>(
                Sort.class,
                PartialDruidQuery.Stage.SORT,
                PartialDruidQuery::withSort
            ),
            new DruidQueryRule<>(
                Project.class,
                PartialDruidQuery.Stage.SORT_PROJECT,
                PartialDruidQuery::withSortProject
            ),
            DruidOuterQueryRule.AGGREGATE,
            DruidOuterQueryRule.WHERE_FILTER,
            DruidOuterQueryRule.SELECT_PROJECT,
            DruidOuterQueryRule.SORT,
            new DruidUnionRule(plannerContext), // Add top level union rule since it helps in constructing a cleaner error message for the user
            new DruidUnionDataSourceRule(plannerContext),
            DruidJoinRule.instance(plannerContext)
        )
    );

    if (plannerContext.featureAvailable(EngineFeature.ALLOW_TOP_LEVEL_UNION_ALL)) {
      retVal.add(DruidSortUnionRule.instance());
    }

    if (plannerContext.featureAvailable(EngineFeature.WINDOW_FUNCTIONS)) {
      retVal.add(new DruidQueryRule<>(Window.class, PartialDruidQuery.Stage.WINDOW, PartialDruidQuery::withWindow));
      retVal.add(
          new DruidQueryRule<>(
              Project.class,
              PartialDruidQuery.Stage.WINDOW_PROJECT,
              Project::isMapping, // We can remap fields, but not apply expressions
              PartialDruidQuery::withWindowProject
          )
      );
      retVal.add(DruidOuterQueryRule.WINDOW);
    }

    // Adding unnest specific rules
    if (plannerContext.featureAvailable(EngineFeature.UNNEST)) {
      retVal.add(new DruidUnnestRule(plannerContext));
      retVal.add(new DruidCorrelateUnnestRule(plannerContext));
      retVal.add(CoreRules.PROJECT_CORRELATE_TRANSPOSE);
      retVal.add(DruidFilterUnnestRule.instance());
      retVal.add(DruidFilterUnnestRule.DruidProjectOnUnnestRule.instance());
    }

    return retVal;
  }

  public static class DruidQueryRule<RelType extends RelNode> extends RelOptRule
  {
    private final PartialDruidQuery.Stage stage;
    private final Predicate<RelType> matchesFn;
    private final BiFunction<PartialDruidQuery, RelType, PartialDruidQuery> applyFn;

    public DruidQueryRule(
        final Class<RelType> relClass,
        final PartialDruidQuery.Stage stage,
        final Predicate<RelType> matchesFn,
        final BiFunction<PartialDruidQuery, RelType, PartialDruidQuery> applyFn
    )
    {
      super(
          operand(relClass, operandJ(DruidRel.class, null, CAN_BUILD_ON, any())),
          StringUtils.format("%s(%s)", DruidQueryRule.class.getSimpleName(), stage)
      );
      this.stage = stage;
      this.matchesFn = matchesFn;
      this.applyFn = applyFn;
    }

    public DruidQueryRule(
        final Class<RelType> relClass,
        final PartialDruidQuery.Stage stage,
        final BiFunction<PartialDruidQuery, RelType, PartialDruidQuery> applyFn
    )
    {
      this(relClass, stage, r -> true, applyFn);
    }

    @Override
    public boolean matches(final RelOptRuleCall call)
    {
      final RelType otherRel = call.rel(0);
      final DruidRel<?> druidRel = call.rel(1);
      return druidRel.getPartialDruidQuery().canAccept(stage) && matchesFn.test(otherRel);
    }

    @Override
    public void onMatch(final RelOptRuleCall call)
    {
      final RelType otherRel = call.rel(0);
      final DruidRel<?> druidRel = call.rel(1);

      final PartialDruidQuery newPartialDruidQuery = applyFn.apply(druidRel.getPartialDruidQuery(), otherRel);
      final DruidRel<?> newDruidRel = druidRel.withPartialQuery(newPartialDruidQuery);

      if (newDruidRel.isValidDruidQuery()) {
        call.transformTo(newDruidRel);
      }
    }
  }

  public abstract static class DruidOuterQueryRule extends RelOptRule
  {
    public static final RelOptRule AGGREGATE = new DruidOuterQueryRule(
        PartialDruidQuery.Stage.AGGREGATE,
        operand(Aggregate.class, operandJ(DruidRel.class, null, CAN_BUILD_ON, any())),
        "AGGREGATE"
    )
    {
      @Override
      public void onMatch(final RelOptRuleCall call)
      {
        final Aggregate aggregate = call.rel(0);
        final DruidRel druidRel = call.rel(1);

        final DruidOuterQueryRel outerQueryRel = DruidOuterQueryRel.create(
            druidRel,
            PartialDruidQuery.createOuterQuery(druidRel.getPartialDruidQuery(), druidRel.getPlannerContext())
                             .withAggregate(aggregate)
        );
        if (outerQueryRel.isValidDruidQuery()) {
          call.transformTo(outerQueryRel);
        }
      }
    };

    public static final RelOptRule WHERE_FILTER = new DruidOuterQueryRule(
        PartialDruidQuery.Stage.WHERE_FILTER,
        operand(Filter.class, operandJ(DruidRel.class, null, CAN_BUILD_ON, any())),
        "WHERE_FILTER"
    )
    {
      @Override
      public void onMatch(final RelOptRuleCall call)
      {
        final Filter filter = call.rel(0);
        final DruidRel druidRel = call.rel(1);

        final DruidOuterQueryRel outerQueryRel = DruidOuterQueryRel.create(
            druidRel,
            PartialDruidQuery.createOuterQuery(druidRel.getPartialDruidQuery(), druidRel.getPlannerContext())
                             .withWhereFilter(filter)
        );
        if (outerQueryRel.isValidDruidQuery()) {
          call.transformTo(outerQueryRel);
        }
      }
    };

    public static final RelOptRule SELECT_PROJECT = new DruidOuterQueryRule(
        PartialDruidQuery.Stage.SELECT_PROJECT,
        operand(Project.class, operandJ(DruidRel.class, null, CAN_BUILD_ON, any())),
        "SELECT_PROJECT"
    )
    {
      @Override
      public void onMatch(final RelOptRuleCall call)
      {
        final Project filter = call.rel(0);
        final DruidRel druidRel = call.rel(1);

        final DruidOuterQueryRel outerQueryRel = DruidOuterQueryRel.create(
            druidRel,
            PartialDruidQuery.createOuterQuery(druidRel.getPartialDruidQuery(), druidRel.getPlannerContext())
                             .withSelectProject(filter)
        );
        if (outerQueryRel.isValidDruidQuery()) {
          call.transformTo(outerQueryRel);
        }
      }
    };

    public static final RelOptRule SORT = new DruidOuterQueryRule(
        PartialDruidQuery.Stage.SORT,
        operand(Sort.class, operandJ(DruidRel.class, null, CAN_BUILD_ON, any())),
        "SORT"
    )
    {
      @Override
      public void onMatch(final RelOptRuleCall call)
      {
        final Sort sort = call.rel(0);
        final DruidRel druidRel = call.rel(1);

        final DruidOuterQueryRel outerQueryRel = DruidOuterQueryRel.create(
            druidRel,
            PartialDruidQuery.createOuterQuery(druidRel.getPartialDruidQuery(), druidRel.getPlannerContext())
                             .withSort(sort)
        );
        if (outerQueryRel.isValidDruidQuery()) {
          call.transformTo(outerQueryRel);
        }
      }
    };

    public static final RelOptRule WINDOW = new DruidOuterQueryRule(
        PartialDruidQuery.Stage.WINDOW,
        operand(Window.class, operandJ(DruidRel.class, null, CAN_BUILD_ON, any())),
        "WINDOW"
    )
    {
      @Override
      public void onMatch(final RelOptRuleCall call)
      {
        final Window window = call.rel(0);
        final DruidRel druidRel = call.rel(1);

        final DruidOuterQueryRel outerQueryRel = DruidOuterQueryRel.create(
            druidRel,
            PartialDruidQuery.createOuterQuery(druidRel.getPartialDruidQuery(), druidRel.getPlannerContext())
                             .withWindow(window)
        );
        if (outerQueryRel.isValidDruidQuery()) {
          call.transformTo(outerQueryRel);
        }
      }
    };

    private final PartialDruidQuery.Stage stage;

    public DruidOuterQueryRule(
        final PartialDruidQuery.Stage stage,
        final RelOptRuleOperand op,
        final String description
    )
    {
      super(op, StringUtils.format("%s(%s)", DruidOuterQueryRel.class.getSimpleName(), description));
      this.stage = stage;
    }

    @Override
    public boolean matches(final RelOptRuleCall call)
    {
      final DruidRel<?> lowerDruidRel = call.rel(call.getRelList().size() - 1);
      final RelNode lowerRel = lowerDruidRel.getPartialDruidQuery().leafRel();
      final PartialDruidQuery.Stage lowerStage = lowerDruidRel.getPartialDruidQuery().stage();

      if (stage.canFollow(lowerStage)
          || (stage == PartialDruidQuery.Stage.WHERE_FILTER
              && PartialDruidQuery.Stage.HAVING_FILTER.canFollow(lowerStage))
          || (stage == PartialDruidQuery.Stage.SELECT_PROJECT
             && PartialDruidQuery.Stage.SORT_PROJECT.canFollow(lowerStage))) {
        // Don't consider cases that can be fused into a single query.
        return false;
      } else if (stage == PartialDruidQuery.Stage.WHERE_FILTER && lowerRel instanceof Filter) {
        // Don't consider filter-on-filter. FilterMergeRule will handle it.
        return false;
      } else if (stage == PartialDruidQuery.Stage.WHERE_FILTER
                 && lowerStage == PartialDruidQuery.Stage.SELECT_PROJECT) {
        // Don't consider filter-on-project. ProjectFilterTransposeRule will handle it by swapping them.
        return false;
      } else if (stage == PartialDruidQuery.Stage.SELECT_PROJECT && lowerRel instanceof Project) {
        // Don't consider project-on-project. ProjectMergeRule will handle it.
        return false;
      } else {
        // Consider subqueries in all other cases.
        return true;
      }
    }
  }
}
