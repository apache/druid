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

package org.apache.druid.sql.calcite.planner;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.rel.rules.AggregateJoinTransposeRule;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rel.rules.AggregateProjectPullUpConstantsRule;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.AggregateRemoveRule;
import org.apache.calcite.rel.rules.AggregateStarTableRule;
import org.apache.calcite.rel.rules.AggregateValuesRule;
import org.apache.calcite.rel.rules.CalcRemoveRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterTableScanRule;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ProjectTableScanRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.ProjectWindowTransposeRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.SortJoinTransposeRule;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.calcite.rel.rules.SortRemoveRule;
import org.apache.calcite.rel.rules.SortUnionTransposeRule;
import org.apache.calcite.rel.rules.SubQueryRemoveRule;
import org.apache.calcite.rel.rules.TableScanRule;
import org.apache.calcite.rel.rules.UnionMergeRule;
import org.apache.calcite.rel.rules.UnionPullUpConstantsRule;
import org.apache.calcite.rel.rules.UnionToDistinctRule;
import org.apache.calcite.rel.rules.ValuesReduceRule;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.druid.sql.calcite.rel.QueryMaker;
import org.apache.druid.sql.calcite.rule.CaseFilteredAggregatorRule;
import org.apache.druid.sql.calcite.rule.DruidRelToDruidRule;
import org.apache.druid.sql.calcite.rule.DruidRules;
import org.apache.druid.sql.calcite.rule.DruidSemiJoinRule;
import org.apache.druid.sql.calcite.rule.DruidTableScanRule;
import org.apache.druid.sql.calcite.rule.ProjectAggregatePruneUnusedCallRule;
import org.apache.druid.sql.calcite.rule.SortCollapseRule;

import java.util.List;

public class Rules
{
  public static final int DRUID_CONVENTION_RULES = 0;
  public static final int BINDABLE_CONVENTION_RULES = 1;

  // Rules from CalcitePrepareImpl's DEFAULT_RULES, minus AggregateExpandDistinctAggregatesRule
  // and AggregateReduceFunctionsRule.
  private static final List<RelOptRule> DEFAULT_RULES =
      ImmutableList.of(
          AggregateStarTableRule.INSTANCE,
          AggregateStarTableRule.INSTANCE2,
          TableScanRule.INSTANCE,
          ProjectMergeRule.INSTANCE,
          FilterTableScanRule.INSTANCE,
          ProjectFilterTransposeRule.INSTANCE,
          FilterProjectTransposeRule.INSTANCE,
          FilterJoinRule.FILTER_ON_JOIN,
          JoinPushExpressionsRule.INSTANCE,
          FilterAggregateTransposeRule.INSTANCE,
          ProjectWindowTransposeRule.INSTANCE,
          JoinCommuteRule.INSTANCE,
          JoinPushThroughJoinRule.RIGHT,
          JoinPushThroughJoinRule.LEFT,
          SortProjectTransposeRule.INSTANCE,
          SortJoinTransposeRule.INSTANCE,
          SortUnionTransposeRule.INSTANCE
      );

  // Rules from CalcitePrepareImpl's createPlanner.
  private static final List<RelOptRule> MISCELLANEOUS_RULES =
      ImmutableList.of(
          Bindables.BINDABLE_TABLE_SCAN_RULE,
          ProjectTableScanRule.INSTANCE,
          ProjectTableScanRule.INTERPRETER
      );

  // Rules from CalcitePrepareImpl's CONSTANT_REDUCTION_RULES.
  private static final List<RelOptRule> CONSTANT_REDUCTION_RULES =
      ImmutableList.of(
          ReduceExpressionsRule.PROJECT_INSTANCE,
          ReduceExpressionsRule.CALC_INSTANCE,
          ReduceExpressionsRule.JOIN_INSTANCE,
          ReduceExpressionsRule.FILTER_INSTANCE,
          ValuesReduceRule.FILTER_INSTANCE,
          ValuesReduceRule.PROJECT_FILTER_INSTANCE,
          ValuesReduceRule.PROJECT_INSTANCE,
          AggregateValuesRule.INSTANCE
      );

  // Rules from VolcanoPlanner's registerAbstractRelationalRules.
  private static final List<RelOptRule> VOLCANO_ABSTRACT_RULES =
      ImmutableList.of(
          FilterJoinRule.FILTER_ON_JOIN,
          FilterJoinRule.JOIN,
          AbstractConverter.ExpandConversionRule.INSTANCE,
          JoinCommuteRule.INSTANCE,
          AggregateRemoveRule.INSTANCE,
          UnionToDistinctRule.INSTANCE,
          ProjectRemoveRule.INSTANCE,
          AggregateJoinTransposeRule.INSTANCE,
          AggregateProjectMergeRule.INSTANCE,
          CalcRemoveRule.INSTANCE,
          SortRemoveRule.INSTANCE
      );

  // Rules from RelOptUtil's registerAbstractRels.
  // Omit DateRangeRules due to https://issues.apache.org/jira/browse/CALCITE-1601
  private static final List<RelOptRule> RELOPTUTIL_ABSTRACT_RULES =
      ImmutableList.of(
          AggregateProjectPullUpConstantsRule.INSTANCE2,
          UnionPullUpConstantsRule.INSTANCE,
          PruneEmptyRules.UNION_INSTANCE,
          PruneEmptyRules.PROJECT_INSTANCE,
          PruneEmptyRules.FILTER_INSTANCE,
          PruneEmptyRules.SORT_INSTANCE,
          PruneEmptyRules.AGGREGATE_INSTANCE,
          PruneEmptyRules.JOIN_LEFT_INSTANCE,
          PruneEmptyRules.JOIN_RIGHT_INSTANCE,
          PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE,
          UnionMergeRule.INSTANCE,
          ProjectToWindowRule.PROJECT,
          FilterMergeRule.INSTANCE
      );

  private static final List<RelOptRule> SUB_QUERY_REMOVE_RULES =
      ImmutableList.of(
          SubQueryRemoveRule.PROJECT,
          SubQueryRemoveRule.FILTER,
          SubQueryRemoveRule.JOIN
      );

  private Rules()
  {
    // No instantiation.
  }

  public static List<Program> programs(final PlannerContext plannerContext, final QueryMaker queryMaker)
  {
    final Program hepProgram =
        Programs.sequence(
            Programs.subQuery(DefaultRelMetadataProvider.INSTANCE),
            new DecorrelateAndTrimFieldsProgram()
        );
    return ImmutableList.of(
        Programs.sequence(hepProgram, Programs.ofRules(druidConventionRuleSet(plannerContext, queryMaker))),
        Programs.sequence(hepProgram, Programs.ofRules(bindableConventionRuleSet(plannerContext)))
    );
  }

  private static List<RelOptRule> druidConventionRuleSet(
      final PlannerContext plannerContext,
      final QueryMaker queryMaker
  )
  {
    final ImmutableList.Builder<RelOptRule> retVal = ImmutableList.<RelOptRule>builder()
        .addAll(baseRuleSet(plannerContext))
        .add(DruidRelToDruidRule.instance())
        .add(new DruidTableScanRule(queryMaker))
        .addAll(DruidRules.rules());

    if (plannerContext.getPlannerConfig().getMaxSemiJoinRowsInMemory() > 0) {
      retVal.add(DruidSemiJoinRule.instance());
    }

    return retVal.build();
  }

  private static List<RelOptRule> bindableConventionRuleSet(final PlannerContext plannerContext)
  {
    return ImmutableList.<RelOptRule>builder()
        .addAll(baseRuleSet(plannerContext))
        .addAll(Bindables.RULES)
        .add(AggregateReduceFunctionsRule.INSTANCE)
        .build();
  }

  private static List<RelOptRule> baseRuleSet(final PlannerContext plannerContext)
  {
    final PlannerConfig plannerConfig = plannerContext.getPlannerConfig();
    final ImmutableList.Builder<RelOptRule> rules = ImmutableList.builder();

    // Calcite rules.
    rules.addAll(DEFAULT_RULES);
    rules.addAll(MISCELLANEOUS_RULES);
    rules.addAll(CONSTANT_REDUCTION_RULES);
    rules.addAll(VOLCANO_ABSTRACT_RULES);
    rules.addAll(RELOPTUTIL_ABSTRACT_RULES);
    rules.addAll(SUB_QUERY_REMOVE_RULES);

    if (!plannerConfig.isUseApproximateCountDistinct()) {
      // We'll need this to expand COUNT DISTINCTs.
      // Avoid AggregateExpandDistinctAggregatesRule.INSTANCE; it uses grouping sets and we don't support those.
      rules.add(AggregateExpandDistinctAggregatesRule.JOIN);
    }

    rules.add(SortCollapseRule.instance());
    rules.add(CaseFilteredAggregatorRule.instance());
    rules.add(ProjectAggregatePruneUnusedCallRule.instance());

    return rules.build();
  }

  // Based on Calcite's Programs.DecorrelateProgram and Programs.TrimFieldsProgram, which are private and only
  // accessible through Programs.standard (which we don't want, since it also adds Enumerable rules).
  private static class DecorrelateAndTrimFieldsProgram implements Program
  {
    @Override
    public RelNode run(
        RelOptPlanner planner,
        RelNode rel,
        RelTraitSet requiredOutputTraits,
        List<RelOptMaterialization> materializations,
        List<RelOptLattice> lattices
    )
    {
      final RelNode decorrelatedRel = RelDecorrelator.decorrelateQuery(rel);
      final RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(decorrelatedRel.getCluster(), null);
      return new RelFieldTrimmer(null, relBuilder).trim(decorrelatedRel);
    }
  }
}
