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

package org.apache.druid.sql.calcite.rule.logical;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Aggregate.Group;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mappings;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Planner rule that recognizes a {@link Aggregate}
 * on top of a {@link Project} and if possible
 * aggregate through the project or removes the project.
 *
 * This is updated version of {@link org.apache.calcite.rel.rules.AggregateProjectMergeRule}
 * to be able to handle expressions
 */
@Value.Enclosing
public class DruidAggregateProjectMergeRule
    extends RelRule<DruidAggregateProjectMergeRule.Config>
    implements TransformationRule
{

  /**
   * Creates a DruidAggregateProjectMergeRule.
   */
  protected DruidAggregateProjectMergeRule(Config config)
  {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public DruidAggregateProjectMergeRule(
      Class<? extends Aggregate> aggregateClass,
      Class<? extends Project> projectClass,
      RelBuilderFactory relBuilderFactory
  )
  {
    this(CoreRules.AGGREGATE_PROJECT_MERGE.config
             .withRelBuilderFactory(relBuilderFactory)
             .as(Config.class)
             .withOperandFor(aggregateClass, projectClass));
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final Aggregate aggregate = call.rel(0);
    final Project project = call.rel(1);
    RelNode x = apply(call, aggregate, project);
    if (x != null) {
      call.transformTo(x);
    }
  }

  public static @Nullable RelNode apply(RelOptRuleCall call, Aggregate aggregate, Project project)
  {
    final Set<Integer> interestingFields = RelOptUtil.getAllFields(aggregate);
    if (interestingFields.isEmpty()) {
      return null;
    }
    final Map<Integer, Integer> map = new HashMap<>();
    final Map<RexNode, Integer> assignedNodeForExpr = new HashMap<>();
    List<RexNode> newRexNodes = new ArrayList<>();
    for (int source : interestingFields) {
      final RexNode rex = project.getProjects().get(source);
      if (!assignedNodeForExpr.containsKey(rex)) {
        RexNode newNode = new RexInputRef(source, rex.getType());
        assignedNodeForExpr.put(rex, newRexNodes.size());
        newRexNodes.add(newNode);
      }
      map.put(source, assignedNodeForExpr.get(rex));
    }

    final ImmutableBitSet newGroupSet = aggregate.getGroupSet().permute(map);
    ImmutableList<ImmutableBitSet> newGroupingSets = null;
    if (aggregate.getGroupType() != Group.SIMPLE) {
      newGroupingSets =
          ImmutableBitSet.ORDERING.immutableSortedCopy(
              Sets.newTreeSet(ImmutableBitSet.permute(aggregate.getGroupSets(), map)));
    }

    final ImmutableList.Builder<AggregateCall> aggCalls = ImmutableList.builder();
    final int sourceCount = aggregate.getInput().getRowType().getFieldCount();
    final int targetCount = newRexNodes.size();
    final Mappings.TargetMapping targetMapping = Mappings.target(map, sourceCount, targetCount);
    for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
      aggCalls.add(aggregateCall.transform(targetMapping));
    }

    final RelBuilder relBuilder = call.builder();
    relBuilder.push(project);
    relBuilder.project(newRexNodes);

    final Aggregate newAggregate =
        aggregate.copy(aggregate.getTraitSet(), relBuilder.build(),
                       newGroupSet, newGroupingSets, aggCalls.build()
        );
    relBuilder.push(newAggregate);

    final List<Integer> newKeys =
        Util.transform(
            aggregate.getGroupSet().asList(),
            key -> Objects.requireNonNull(
                map.get(key),
                () -> "no value found for key " + key + " in " + map
            )
        );

    // Add a project if the group set is not in the same order or
    // contains duplicates.
    if (!newKeys.equals(newGroupSet.asList())) {
      final List<Integer> posList = new ArrayList<>();
      for (int newKey : newKeys) {
        posList.add(newGroupSet.indexOf(newKey));
      }
      for (int i = newAggregate.getGroupCount();
           i < newAggregate.getRowType().getFieldCount(); i++) {
        posList.add(i);
      }
      relBuilder.project(relBuilder.fields(posList));
    }

    return relBuilder.build();
  }

  /**
   * Rule configuration.
   */
  @Value.Immutable
  public interface Config extends RelRule.Config
  {
    Config DEFAULT = DruidImmutableAggregateProjectMergeRule.Config.of()
                                                                   .withOperandFor(Aggregate.class, Project.class);

    @Override
    default DruidAggregateProjectMergeRule toRule()
    {
      return new DruidAggregateProjectMergeRule(this);
    }

    /**
     * Defines an operand tree for the given classes.
     */
    default Config withOperandFor(
        Class<? extends Aggregate> aggregateClass,
        Class<? extends Project> projectClass
    )
    {
      return withOperandSupplier(b0 ->
                                     b0.operand(aggregateClass).oneInput(b1 ->
                                                                             b1.operand(projectClass).anyInputs())).as(
          Config.class);
    }
  }
}
