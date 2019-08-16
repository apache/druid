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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Accumulator;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * DruidRel that has a main query, and also a subquery "right" that is used to filter the main query.
 */
public class DruidSemiJoin extends DruidRel<DruidSemiJoin>
{
  private final List<RexNode> leftExpressions;
  private final List<Integer> rightKeys;
  private final int maxSemiJoinRowsInMemory;
  private DruidRel<?> left;
  private RelNode right;

  private DruidSemiJoin(
      final RelOptCluster cluster,
      final RelTraitSet traitSet,
      final DruidRel<?> left,
      final RelNode right,
      final List<RexNode> leftExpressions,
      final List<Integer> rightKeys,
      final int maxSemiJoinRowsInMemory,
      final QueryMaker queryMaker
  )
  {
    super(cluster, traitSet, queryMaker);
    this.left = left;
    this.right = right;
    this.leftExpressions = ImmutableList.copyOf(leftExpressions);
    this.rightKeys = ImmutableList.copyOf(rightKeys);
    this.maxSemiJoinRowsInMemory = maxSemiJoinRowsInMemory;
  }

  public static DruidSemiJoin create(
      final DruidRel left,
      final DruidRel right,
      final List<Integer> leftKeys,
      final List<Integer> rightKeys,
      final PlannerContext plannerContext
  )
  {
    final ImmutableList.Builder<RexNode> listBuilder = ImmutableList.builder();

    final PartialDruidQuery leftPartialQuery = left.getPartialDruidQuery();
    if (leftPartialQuery.stage().compareTo(PartialDruidQuery.Stage.AGGREGATE) >= 0) {
      throw new ISE("LHS must not be an Aggregate");
    }

    if (leftPartialQuery.getSelectProject() != null) {
      for (int key : leftKeys) {
        listBuilder.add(leftPartialQuery.getSelectProject().getChildExps().get(key));
      }
    } else {
      for (int key : leftKeys) {
        listBuilder.add(RexInputRef.of(key, leftPartialQuery.getRowType()));
      }
    }

    return new DruidSemiJoin(
        left.getCluster(),
        left.getTraitSet(),
        left,
        right,
        listBuilder.build(),
        rightKeys,
        plannerContext.getPlannerConfig().getMaxSemiJoinRowsInMemory(),
        left.getQueryMaker()
    );
  }

  @Override
  public PartialDruidQuery getPartialDruidQuery()
  {
    return left.getPartialDruidQuery();
  }

  @Override
  public DruidSemiJoin withPartialQuery(final PartialDruidQuery newQueryBuilder)
  {
    return new DruidSemiJoin(
        getCluster(),
        getTraitSet().plusAll(newQueryBuilder.getRelTraits()),
        left.withPartialQuery(newQueryBuilder),
        right,
        leftExpressions,
        rightKeys,
        maxSemiJoinRowsInMemory,
        getQueryMaker()
    );
  }

  @Nullable
  @Override
  public DruidQuery toDruidQuery(final boolean finalizeAggregations)
  {
    final DruidRel rel = getLeftRelWithFilter();
    return rel != null ? rel.toDruidQuery(finalizeAggregations) : null;
  }

  @Override
  public DruidQuery toDruidQueryForExplaining()
  {
    return left.toDruidQueryForExplaining();
  }

  @Override
  public DruidSemiJoin asDruidConvention()
  {
    return new DruidSemiJoin(
        getCluster(),
        getTraitSet().replace(DruidConvention.instance()),
        left,
        RelOptRule.convert(right, DruidConvention.instance()),
        leftExpressions,
        rightKeys,
        maxSemiJoinRowsInMemory,
        getQueryMaker()
    );
  }

  @Override
  public List<String> getDataSourceNames()
  {
    final DruidRel<?> druidRight = (DruidRel) this.right;
    Set<String> datasourceNames = new LinkedHashSet<>();
    datasourceNames.addAll(left.getDataSourceNames());
    datasourceNames.addAll(druidRight.getDataSourceNames());
    return new ArrayList<>(datasourceNames);
  }

  @Override
  public int getQueryCount()
  {
    return left.getQueryCount() + ((DruidRel) right).getQueryCount();
  }

  @Override
  public Sequence<Object[]> runQuery()
  {
    final DruidRel<?> rel = getLeftRelWithFilter();
    if (rel != null) {
      return rel.runQuery();
    } else {
      return Sequences.empty();
    }
  }

  @Override
  protected RelDataType deriveRowType()
  {
    return left.getRowType();
  }

  @Override
  public List<RelNode> getInputs()
  {
    return ImmutableList.of(right);
  }

  @Override
  public void replaceInput(int ordinalInParent, RelNode p)
  {
    if (ordinalInParent != 0) {
      throw new IndexOutOfBoundsException(StringUtils.format("Invalid ordinalInParent[%s]", ordinalInParent));
    }
    // 'right' is the only one Calcite concerns. See getInputs().
    this.right = p;
  }

  @Override
  public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs)
  {
    return new DruidSemiJoin(
        getCluster(),
        getTraitSet(),
        left,
        Iterables.getOnlyElement(inputs),
        leftExpressions,
        rightKeys,
        maxSemiJoinRowsInMemory,
        getQueryMaker()
    );
  }

  @Override
  public RelWriter explainTerms(RelWriter pw)
  {
    final String queryString;

    try {
      queryString = getQueryMaker().getJsonMapper().writeValueAsString(toDruidQueryForExplaining().getQuery());
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    return super.explainTerms(pw)
                .input("right", right)
                .item("query", queryString)
                .item("leftExpressions", leftExpressions)
                .item("rightKeys", rightKeys);
  }

  @Override
  public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq)
  {
    return right.computeSelfCost(planner, mq).plus(left.computeSelfCost(planner, mq).multiplyBy(50));
  }

  /**
   * Returns a copy of the left rel with the filter applied from the right-hand side. This is an expensive operation
   * since it actually executes the right-hand side query.
   */
  private DruidRel<?> getLeftRelWithFilter()
  {
    final DruidRel<?> druidRight = (DruidRel) this.right;

    // Build list of acceptable values from right side.
    final Set<List<String>> valuess = new HashSet<>();
    final List<RexNode> conditions = druidRight.runQuery().accumulate(
        new ArrayList<>(),
        new Accumulator<List<RexNode>, Object[]>()
        {
          int numRows;

          @Override
          public List<RexNode> accumulate(final List<RexNode> theConditions, final Object[] row)
          {
            final List<String> values = new ArrayList<>(rightKeys.size());

            for (int i : rightKeys) {
              final Object value = row[i];
              if (value == null) {
                // NULLs are not supposed to match NULLs in a join. So ignore them.
                continue;
              }
              final String stringValue = DimensionHandlerUtils.convertObjectToString(value);
              values.add(stringValue);
            }

            if (valuess.add(values)) {
              if (++numRows > maxSemiJoinRowsInMemory) {
                throw new ResourceLimitExceededException(
                    StringUtils.format("maxSemiJoinRowsInMemory[%,d] exceeded", maxSemiJoinRowsInMemory)
                );
              }
              final List<RexNode> subConditions = new ArrayList<>();

              for (int i = 0; i < values.size(); i++) {
                final String value = values.get(i);
                // NULLs are not supposed to match NULLs in a join. So ignore them.
                if (value != null) {
                  subConditions.add(
                      getCluster().getRexBuilder().makeCall(
                          SqlStdOperatorTable.EQUALS,
                          leftExpressions.get(i),
                          getCluster().getRexBuilder().makeLiteral(value)
                      )
                  );
                }
                theConditions.add(makeAnd(subConditions));
              }
            }
            return theConditions;
          }
        }
    );

    valuess.clear();

    if (!conditions.isEmpty()) {
      // Add a filter to the left side.
      final PartialDruidQuery leftPartialQuery = left.getPartialDruidQuery();
      final Filter whereFilter = leftPartialQuery.getWhereFilter();
      final Filter newWhereFilter;

      if (whereFilter != null) {
        newWhereFilter = whereFilter.copy(
            whereFilter.getTraitSet(),
            whereFilter.getInput(),
            RexUtil.flatten(
                getCluster().getRexBuilder(),
                makeAnd(ImmutableList.of(whereFilter.getCondition(), makeOr(conditions)))
            )
        );
      } else {
        newWhereFilter = LogicalFilter.create(
            leftPartialQuery.getScan(),
            makeOr(conditions) // already in flattened form
        );
      }

      PartialDruidQuery newPartialQuery = PartialDruidQuery.create(leftPartialQuery.getScan())
                                                           .withWhereFilter(newWhereFilter)
                                                           .withSelectProject(leftPartialQuery.getSelectProject());

      if (leftPartialQuery.getAggregate() != null) {
        newPartialQuery = newPartialQuery.withAggregate(leftPartialQuery.getAggregate());
      }

      if (leftPartialQuery.getHavingFilter() != null) {
        newPartialQuery = newPartialQuery.withHavingFilter(leftPartialQuery.getHavingFilter());
      }

      if (leftPartialQuery.getAggregateProject() != null) {
        newPartialQuery = newPartialQuery.withAggregateProject(leftPartialQuery.getAggregateProject());
      }

      if (leftPartialQuery.getSort() != null) {
        newPartialQuery = newPartialQuery.withSort(leftPartialQuery.getSort());
      }

      if (leftPartialQuery.getSortProject() != null) {
        newPartialQuery = newPartialQuery.withSortProject(leftPartialQuery.getSortProject());
      }

      return left.withPartialQuery(newPartialQuery);
    } else {
      return null;
    }
  }

  private RexNode makeAnd(final List<RexNode> conditions)
  {
    if (conditions.size() == 1) {
      return Iterables.getOnlyElement(conditions);
    } else {
      return getCluster().getRexBuilder().makeCall(SqlStdOperatorTable.AND, conditions);
    }
  }

  private RexNode makeOr(final List<RexNode> conditions)
  {
    if (conditions.size() == 1) {
      return Iterables.getOnlyElement(conditions);
    } else {
      return getCluster().getRexBuilder().makeCall(SqlStdOperatorTable.OR, conditions);
    }
  }
}
