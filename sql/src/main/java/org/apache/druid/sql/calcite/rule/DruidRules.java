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

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.sql.calcite.rel.DruidOuterQueryRel;
import org.apache.druid.sql.calcite.rel.DruidRel;
import org.apache.druid.sql.calcite.rel.PartialDruidQuery;

import java.util.List;
import java.util.function.BiFunction;

public class DruidRules
{
  public static final Predicate<DruidRel> CAN_BUILD_ON = druidRel -> druidRel.getPartialDruidQuery() != null;

  private DruidRules()
  {
    // No instantiation.
  }

  public static List<RelOptRule> rules()
  {
    return ImmutableList.of(
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
        DruidOuterQueryRule.FILTER_AGGREGATE,
        DruidOuterQueryRule.FILTER_PROJECT_AGGREGATE,
        DruidOuterQueryRule.PROJECT_AGGREGATE,
        DruidOuterQueryRule.AGGREGATE_SORT_PROJECT,
        DruidUnionRule.instance(),
        DruidSortUnionRule.instance()
    );
  }

  public static class DruidQueryRule<RelType extends RelNode> extends RelOptRule
  {
    private final PartialDruidQuery.Stage stage;
    private final BiFunction<PartialDruidQuery, RelType, PartialDruidQuery> f;

    public DruidQueryRule(
        final Class<RelType> relClass,
        final PartialDruidQuery.Stage stage,
        final BiFunction<PartialDruidQuery, RelType, PartialDruidQuery> f
    )
    {
      super(
          operand(relClass, operand(DruidRel.class, null, CAN_BUILD_ON, any())),
          StringUtils.format("%s(%s)", DruidQueryRule.class.getSimpleName(), stage)
      );
      this.stage = stage;
      this.f = f;
    }

    @Override
    public boolean matches(final RelOptRuleCall call)
    {
      final DruidRel druidRel = call.rel(1);
      return druidRel.getPartialDruidQuery().canAccept(stage);
    }

    @Override
    public void onMatch(final RelOptRuleCall call)
    {
      final RelType otherRel = call.rel(0);
      final DruidRel druidRel = call.rel(1);

      final PartialDruidQuery newPartialDruidQuery = f.apply(druidRel.getPartialDruidQuery(), otherRel);
      final DruidRel newDruidRel = druidRel.withPartialQuery(newPartialDruidQuery);

      if (newDruidRel.isValidDruidQuery()) {
        call.transformTo(newDruidRel);
      }
    }
  }

  public abstract static class DruidOuterQueryRule extends RelOptRule
  {
    public static final RelOptRule AGGREGATE = new DruidOuterQueryRule(
        operand(Aggregate.class, operand(DruidRel.class, null, CAN_BUILD_ON, any())),
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
            PartialDruidQuery.create(druidRel.getPartialDruidQuery().leafRel())
                             .withAggregate(aggregate)
        );
        if (outerQueryRel.isValidDruidQuery()) {
          call.transformTo(outerQueryRel);
        }
      }
    };

    public static final RelOptRule FILTER_AGGREGATE = new DruidOuterQueryRule(
        operand(Aggregate.class, operand(Filter.class, operand(DruidRel.class, null, CAN_BUILD_ON, any()))),
        "FILTER_AGGREGATE"
    )
    {
      @Override
      public void onMatch(final RelOptRuleCall call)
      {
        final Aggregate aggregate = call.rel(0);
        final Filter filter = call.rel(1);
        final DruidRel druidRel = call.rel(2);

        final DruidOuterQueryRel outerQueryRel = DruidOuterQueryRel.create(
            druidRel,
            PartialDruidQuery.create(druidRel.getPartialDruidQuery().leafRel())
                             .withWhereFilter(filter)
                             .withAggregate(aggregate)
        );
        if (outerQueryRel.isValidDruidQuery()) {
          call.transformTo(outerQueryRel);
        }
      }
    };

    public static final RelOptRule FILTER_PROJECT_AGGREGATE = new DruidOuterQueryRule(
        operand(
            Aggregate.class,
            operand(Project.class, operand(Filter.class, operand(DruidRel.class, null, CAN_BUILD_ON, any())))
        ),
        "FILTER_PROJECT_AGGREGATE"
    )
    {
      @Override
      public void onMatch(final RelOptRuleCall call)
      {
        final Aggregate aggregate = call.rel(0);
        final Project project = call.rel(1);
        final Filter filter = call.rel(2);
        final DruidRel druidRel = call.rel(3);

        final DruidOuterQueryRel outerQueryRel = DruidOuterQueryRel.create(
            druidRel,
            PartialDruidQuery.create(druidRel.getPartialDruidQuery().leafRel())
                             .withWhereFilter(filter)
                             .withSelectProject(project)
                             .withAggregate(aggregate)
        );
        if (outerQueryRel.isValidDruidQuery()) {
          call.transformTo(outerQueryRel);
        }
      }
    };

    public static final RelOptRule PROJECT_AGGREGATE = new DruidOuterQueryRule(
        operand(Aggregate.class, operand(Project.class, operand(DruidRel.class, null, CAN_BUILD_ON, any()))),
        "PROJECT_AGGREGATE"
    )
    {
      @Override
      public void onMatch(final RelOptRuleCall call)
      {
        final Aggregate aggregate = call.rel(0);
        final Project project = call.rel(1);
        final DruidRel druidRel = call.rel(2);

        final DruidOuterQueryRel outerQueryRel = DruidOuterQueryRel.create(
            druidRel,
            PartialDruidQuery.create(druidRel.getPartialDruidQuery().leafRel())
                             .withSelectProject(project)
                             .withAggregate(aggregate)
        );
        if (outerQueryRel.isValidDruidQuery()) {
          call.transformTo(outerQueryRel);
        }
      }
    };

    public static final RelOptRule AGGREGATE_SORT_PROJECT = new DruidOuterQueryRule(
        operand(
            Project.class,
            operand(Sort.class, operand(Aggregate.class, operand(DruidRel.class, null, CAN_BUILD_ON, any())))
        ),
        "AGGREGATE_SORT_PROJECT"
    )
    {
      @Override
      public void onMatch(RelOptRuleCall call)
      {
        final Project sortProject = call.rel(0);
        final Sort sort = call.rel(1);
        final Aggregate aggregate = call.rel(2);
        final DruidRel druidRel = call.rel(3);

        final DruidOuterQueryRel outerQueryRel = DruidOuterQueryRel.create(
            druidRel,
            PartialDruidQuery.create(druidRel.getPartialDruidQuery().leafRel())
                             .withAggregate(aggregate)
                             .withSort(sort)
                             .withSortProject(sortProject)
        );
        if (outerQueryRel.isValidDruidQuery()) {
          call.transformTo(outerQueryRel);
        }
      }
    };

    public DruidOuterQueryRule(final RelOptRuleOperand op, final String description)
    {
      super(op, StringUtils.format("%s(%s)", DruidOuterQueryRel.class.getSimpleName(), description));
    }

    @Override
    public boolean matches(final RelOptRuleCall call)
    {
      // Subquery must be a groupBy, so stage must be >= AGGREGATE.
      final DruidRel druidRel = call.rel(call.getRelList().size() - 1);
      return druidRel.getPartialDruidQuery().stage().compareTo(PartialDruidQuery.Stage.AGGREGATE) >= 0;
    }
  }
}
