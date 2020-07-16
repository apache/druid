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
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.rules.AggregateCaseToFilterRule;
import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.rel.rules.AggregateJoinTransposeRule;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rel.rules.AggregateProjectPullUpConstantsRule;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.AggregateRemoveRule;
import org.apache.calcite.rel.rules.AggregateStarTableRule;
import org.apache.calcite.rel.rules.AggregateValuesRule;
import org.apache.calcite.rel.rules.CalcRemoveRule;
import org.apache.calcite.rel.rules.ExchangeRemoveConstantKeysRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterTableScanRule;
import org.apache.calcite.rel.rules.IntersectToDistinctRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.MatchRule;
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
import org.apache.calcite.rel.rules.SortRemoveConstantKeysRule;
import org.apache.calcite.rel.rules.SortRemoveRule;
import org.apache.calcite.rel.rules.SortUnionTransposeRule;
import org.apache.calcite.rel.rules.TableScanRule;
import org.apache.calcite.rel.rules.UnionPullUpConstantsRule;
import org.apache.calcite.rel.rules.UnionToDistinctRule;
import org.apache.calcite.rel.rules.ValuesReduceRule;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.druid.sql.calcite.rel.QueryMaker;
import org.apache.druid.sql.calcite.rule.DruidRelToDruidRule;
import org.apache.druid.sql.calcite.rule.DruidRules;
import org.apache.druid.sql.calcite.rule.DruidTableScanRule;
import org.apache.druid.sql.calcite.rule.FilterJoinExcludePushToChildRule;
import org.apache.druid.sql.calcite.rule.ProjectAggregatePruneUnusedCallRule;
import org.apache.druid.sql.calcite.rule.SortCollapseRule;

import java.util.List;

public class Rules
{
  public static final int DRUID_CONVENTION_RULES = 0;
  public static final int BINDABLE_CONVENTION_RULES = 1;

  // Due to Calcite bug (CALCITE-3845), ReduceExpressionsRule can considered expression which is the same as the
  // previous input expression as reduced. Basically, the expression is actually not reduced but is still considered as
  // reduced. Hence, this resulted in an infinite loop of Calcite trying to reducing the same expression over and over.
  // Calcite 1.23.0 fixes this issue by not consider expression as reduced if this case happens. However, while
  // we are still using Calcite 1.21.0, a workaround is to limit the number of pattern matches to avoid infinite loop.
  private static final String HEP_DEFAULT_MATCH_LIMIT_CONFIG_STRING = "druid.sql.planner.hepMatchLimit";
  private static final int HEP_DEFAULT_MATCH_LIMIT = Integer.valueOf(
      System.getProperty(HEP_DEFAULT_MATCH_LIMIT_CONFIG_STRING, "1200")
  );

  // Rules from RelOptUtil's registerBaseRules, minus:
  //
  // 1) AggregateExpandDistinctAggregatesRule (it'll be added back later if approximate count distinct is disabled)
  // 2) AggregateReduceFunctionsRule (it'll be added back for the Bindable rule set, but we don't want it for Druid
  //    rules since it expands AVG, STDDEV, VAR, etc, and we have aggregators specifically designed for those
  //    functions).
  // 3) JoinCommuteRule (we don't support reordering joins yet).
  // 4) JoinPushThroughJoinRule (we don't support reordering joins yet).
  private static final List<RelOptRule> BASE_RULES =
      ImmutableList.of(
          AggregateStarTableRule.INSTANCE,
          AggregateStarTableRule.INSTANCE2,
          TableScanRule.INSTANCE,
          ProjectMergeRule.INSTANCE,
          FilterTableScanRule.INSTANCE,
          ProjectFilterTransposeRule.INSTANCE,
          FilterProjectTransposeRule.INSTANCE,
          JoinPushExpressionsRule.INSTANCE,
          AggregateCaseToFilterRule.INSTANCE,
          FilterAggregateTransposeRule.INSTANCE,
          ProjectWindowTransposeRule.INSTANCE,
          MatchRule.INSTANCE,
          SortProjectTransposeRule.INSTANCE,
          SortJoinTransposeRule.INSTANCE,
          SortRemoveConstantKeysRule.INSTANCE,
          SortUnionTransposeRule.INSTANCE,
          ExchangeRemoveConstantKeysRule.EXCHANGE_INSTANCE,
          ExchangeRemoveConstantKeysRule.SORT_EXCHANGE_INSTANCE
      );

  // Rules for scanning via Bindable, embedded directly in RelOptUtil's registerDefaultRules.
  private static final List<RelOptRule> DEFAULT_BINDABLE_RULES =
      ImmutableList.of(
          Bindables.BINDABLE_TABLE_SCAN_RULE,
          ProjectTableScanRule.INSTANCE,
          ProjectTableScanRule.INTERPRETER
      );

  // Rules from RelOptUtil's registerReductionRules, minus:
  //
  // 1) ReduceExpressionsRule.JOIN_INSTANCE
  //    Removed by https://github.com/apache/druid/pull/9941 due to issue in https://github.com/apache/druid/issues/9942
  //    TODO: Re-enable when https://github.com/apache/druid/issues/9942 is fixed
  private static final List<RelOptRule> REDUCTION_RULES =
      ImmutableList.of(
          ReduceExpressionsRule.PROJECT_INSTANCE,
          ReduceExpressionsRule.FILTER_INSTANCE,
          ReduceExpressionsRule.CALC_INSTANCE,
          ReduceExpressionsRule.WINDOW_INSTANCE,
          ValuesReduceRule.FILTER_INSTANCE,
          ValuesReduceRule.PROJECT_FILTER_INSTANCE,
          ValuesReduceRule.PROJECT_INSTANCE,
          AggregateValuesRule.INSTANCE
      );

  // Rules from RelOptUtil's registerAbstractRules.
  // Omit DateRangeRules due to https://issues.apache.org/jira/browse/CALCITE-1601
  // Omit UnionMergeRule since it isn't very effective given how Druid unions currently operate and is potentially
  // expensive in terms of planning time.
  private static final List<RelOptRule> ABSTRACT_RULES =
      ImmutableList.of(
          AggregateProjectPullUpConstantsRule.INSTANCE2,
          UnionPullUpConstantsRule.INSTANCE,
          PruneEmptyRules.UNION_INSTANCE,
          PruneEmptyRules.INTERSECT_INSTANCE,
          PruneEmptyRules.MINUS_INSTANCE,
          PruneEmptyRules.PROJECT_INSTANCE,
          PruneEmptyRules.FILTER_INSTANCE,
          PruneEmptyRules.SORT_INSTANCE,
          PruneEmptyRules.AGGREGATE_INSTANCE,
          PruneEmptyRules.JOIN_LEFT_INSTANCE,
          PruneEmptyRules.JOIN_RIGHT_INSTANCE,
          PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE,
          ProjectToWindowRule.PROJECT,
          FilterMergeRule.INSTANCE,
          IntersectToDistinctRule.INSTANCE
      );

  // Rules from RelOptUtil's registerAbstractRelationalRules, minus:
  //
  // 1) AggregateMergeRule (it causes testDoubleNestedGroupBy2 to fail)
  // 2) SemiJoinRule.PROJECT and SemiJoinRule.JOIN (we don't need to detect semi-joins, because they are handled
  //    fine as-is by DruidJoinRule).
  // 3) JoinCommuteRule (we don't support reordering joins yet).
  // 4) FilterJoinRule.FILTER_ON_JOIN and FilterJoinRule.JOIN
  //    Removed by https://github.com/apache/druid/pull/9773 due to issue in https://github.com/apache/druid/issues/9843
  //    TODO: Re-enable when https://github.com/apache/druid/issues/9843 is fixed
  private static final List<RelOptRule> ABSTRACT_RELATIONAL_RULES =
      ImmutableList.of(
          AbstractConverter.ExpandConversionRule.INSTANCE,
          AggregateRemoveRule.INSTANCE,
          UnionToDistinctRule.INSTANCE,
          ProjectRemoveRule.INSTANCE,
          AggregateJoinTransposeRule.INSTANCE,
          AggregateProjectMergeRule.INSTANCE,
          CalcRemoveRule.INSTANCE,
          SortRemoveRule.INSTANCE
      );

  private Rules()
  {
    // No instantiation.
  }

  public static List<Program> programs(final PlannerContext plannerContext, final QueryMaker queryMaker)
  {


    // Program that pre-processes the tree before letting the full-on VolcanoPlanner loose.
    final Program preProgram =
        Programs.sequence(
            Programs.subQuery(DefaultRelMetadataProvider.INSTANCE),
            DecorrelateAndTrimFieldsProgram.INSTANCE,
            buildHepProgram(REDUCTION_RULES, true, DefaultRelMetadataProvider.INSTANCE, HEP_DEFAULT_MATCH_LIMIT)
        );

    return ImmutableList.of(
        Programs.sequence(preProgram, Programs.ofRules(druidConventionRuleSet(plannerContext, queryMaker))),
        Programs.sequence(preProgram, Programs.ofRules(bindableConventionRuleSet(plannerContext)))
    );
  }

  private static Program buildHepProgram(Iterable<? extends RelOptRule> rules,
                                         boolean noDag,
                                         RelMetadataProvider metadataProvider,
                                         int matchLimit)
  {
    final HepProgramBuilder builder = HepProgram.builder();
    builder.addMatchLimit(matchLimit);
    for (RelOptRule rule : rules) {
      builder.addRuleInstance(rule);
    }
    return Programs.of(builder.build(), noDag, metadataProvider);
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

    return retVal.build();
  }

  private static List<RelOptRule> bindableConventionRuleSet(final PlannerContext plannerContext)
  {
    return ImmutableList.<RelOptRule>builder()
        .addAll(baseRuleSet(plannerContext))
        .addAll(Bindables.RULES)
        .addAll(DEFAULT_BINDABLE_RULES)
        .add(AggregateReduceFunctionsRule.INSTANCE)
        .build();
  }

  private static List<RelOptRule> baseRuleSet(final PlannerContext plannerContext)
  {
    final PlannerConfig plannerConfig = plannerContext.getPlannerConfig();
    final ImmutableList.Builder<RelOptRule> rules = ImmutableList.builder();

    // Calcite rules.
    rules.addAll(BASE_RULES);
    rules.addAll(ABSTRACT_RULES);
    rules.addAll(ABSTRACT_RELATIONAL_RULES);

    if (!plannerConfig.isUseApproximateCountDistinct()) {
      // For some reason, even though we support grouping sets, using AggregateExpandDistinctAggregatesRule.INSTANCE
      // here causes CalciteQueryTest#testExactCountDistinctWithGroupingAndOtherAggregators to fail.
      rules.add(AggregateExpandDistinctAggregatesRule.JOIN);
    }

    // Rules that we wrote.
    rules.add(FilterJoinExcludePushToChildRule.FILTER_ON_JOIN_EXCLUDE_PUSH_TO_CHILD);
    rules.add(SortCollapseRule.instance());
    rules.add(ProjectAggregatePruneUnusedCallRule.instance());

    return rules.build();
  }

  // Based on Calcite's Programs.DecorrelateProgram and Programs.TrimFieldsProgram, which are private and only
  // accessible through Programs.standard (which we don't want, since it also adds Enumerable rules).
  private static class DecorrelateAndTrimFieldsProgram implements Program
  {
    private static final DecorrelateAndTrimFieldsProgram INSTANCE = new DecorrelateAndTrimFieldsProgram();

    @Override
    public RelNode run(
        RelOptPlanner planner,
        RelNode rel,
        RelTraitSet requiredOutputTraits,
        List<RelOptMaterialization> materializations,
        List<RelOptLattice> lattices
    )
    {
      final RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
      final RelNode decorrelatedRel = RelDecorrelator.decorrelateQuery(rel, relBuilder);
      return new RelFieldTrimmer(null, relBuilder).trim(decorrelatedRel);
    }
  }
}
