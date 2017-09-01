/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.rel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.QueryDataSource;
import io.druid.query.ResourceLimitExceededException;
import io.druid.query.TableDataSource;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.ExpressionDimFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.segment.VirtualColumn;
import io.druid.segment.virtual.ExpressionVirtualColumn;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * DruidRel that has a main query, and also a subquery "right" that is used to filter the main query.
 */
public class DruidSemiJoin extends DruidRel<DruidSemiJoin>
{
  private final DruidRel<?> left;
  private final RelNode right;
  private final List<DruidExpression> leftExpressions;
  private final List<Integer> rightKeys;
  private final int maxSemiJoinRowsInMemory;

  private DruidSemiJoin(
      final RelOptCluster cluster,
      final RelTraitSet traitSet,
      final DruidRel<?> left,
      final RelNode right,
      final List<DruidExpression> leftExpressions,
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

  public static DruidSemiJoin from(
      final DruidRel left,
      final DruidRel right,
      final List<Integer> leftKeys,
      final List<Integer> rightKeys,
      final PlannerContext plannerContext
  )
  {
    final ImmutableList.Builder<DruidExpression> listBuilder = ImmutableList.builder();
    for (Integer key : leftKeys) {
      final String columnName = left.getQueryBuilder().getRowOrder().get(key);

      final VirtualColumn leftVirtualColumn = left.getQueryBuilder()
                                                  .getVirtualColumns(plannerContext.getExprMacroTable())
                                                  .getVirtualColumn(columnName);

      if (leftVirtualColumn != null) {
        // VirtualColumns not allowed to remain in "left" since we have no way of forcing later rules to include them.
        // See if we can get rid of this virtual column reference, otherwise give up.
        if (leftVirtualColumn instanceof ExpressionVirtualColumn) {
          final ExpressionVirtualColumn expressionColumn = (ExpressionVirtualColumn) leftVirtualColumn;
          listBuilder.add(DruidExpression.fromExpression(expressionColumn.getExpression()));
        } else {
          return null;
        }
      } else {
        listBuilder.add(DruidExpression.fromColumn(columnName));
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
  public RowSignature getSourceRowSignature()
  {
    return left.getSourceRowSignature();
  }

  @Override
  public DruidQueryBuilder getQueryBuilder()
  {
    return left.getQueryBuilder();
  }

  @Override
  public DruidSemiJoin withQueryBuilder(final DruidQueryBuilder newQueryBuilder)
  {
    return new DruidSemiJoin(
        getCluster(),
        getTraitSet().plusAll(newQueryBuilder.getRelTraits()),
        left.withQueryBuilder(newQueryBuilder),
        right,
        leftExpressions,
        rightKeys,
        maxSemiJoinRowsInMemory,
        getQueryMaker()
    );
  }

  @Nullable
  @Override
  public QueryDataSource asDataSource()
  {
    final DruidRel rel = getLeftRelWithFilter();
    return rel != null ? rel.asDataSource() : null;
  }

  @Override
  public DruidSemiJoin asBindable()
  {
    return new DruidSemiJoin(
        getCluster(),
        getTraitSet().replace(BindableConvention.INSTANCE),
        left,
        RelOptRule.convert(right, BindableConvention.INSTANCE),
        leftExpressions,
        rightKeys,
        maxSemiJoinRowsInMemory,
        getQueryMaker()
    );
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
  public int getQueryCount()
  {
    return ((DruidRel) left).getQueryCount() + ((DruidRel) right).getQueryCount();
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
    final TableDataSource dummyDataSource = new TableDataSource("__subquery__");
    final String queryString;

    try {
      queryString = getQueryMaker()
          .getJsonMapper()
          .writeValueAsString(left.getQueryBuilder().toQuery(dummyDataSource, getPlannerContext()));
    }
    catch (IOException e) {
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
    final Set<List<String>> valuess = Sets.newHashSet();
    final List<DimFilter> filters = druidRight.runQuery().accumulate(
        new ArrayList<>(),
        new Accumulator<List<DimFilter>, Object[]>()
        {
          @Override
          public List<DimFilter> accumulate(final List<DimFilter> theFilters, final Object[] row)
          {
            final List<String> values = Lists.newArrayListWithCapacity(rightKeys.size());

            for (int i : rightKeys) {
              final Object value = row[i];
              final String stringValue = value != null ? String.valueOf(value) : "";
              values.add(stringValue);
              if (values.size() > maxSemiJoinRowsInMemory) {
                throw new ResourceLimitExceededException(
                    StringUtils.format("maxSemiJoinRowsInMemory[%,d] exceeded", maxSemiJoinRowsInMemory)
                );
              }
            }

            if (valuess.add(values)) {
              final List<DimFilter> bounds = Lists.newArrayList();
              for (int i = 0; i < values.size(); i++) {
                final DruidExpression leftExpression = leftExpressions.get(i);
                if (leftExpression.isSimpleExtraction()) {
                  bounds.add(
                      new BoundDimFilter(
                          leftExpression.getSimpleExtraction().getColumn(),
                          values.get(i),
                          values.get(i),
                          false,
                          false,
                          null,
                          leftExpression.getSimpleExtraction().getExtractionFn(),
                          getSourceRowSignature().naturalStringComparator(leftExpression.getSimpleExtraction())
                      )
                  );
                } else {
                  bounds.add(
                      new ExpressionDimFilter(
                          StringUtils.format(
                              "(%s == %s)",
                              leftExpression.getExpression(),
                              DruidExpression.stringLiteral(values.get(i))
                          ),
                          getPlannerContext().getExprMacroTable()
                      )
                  );
                }
              }
              theFilters.add(new AndDimFilter(bounds));
            }
            return theFilters;
          }
        }
    );

    valuess.clear();

    if (!filters.isEmpty()) {
      // Add a filter to the left side.
      final DimFilter semiJoinFilter = new OrDimFilter(filters);
      final DimFilter newFilter = left.getQueryBuilder().getFilter() == null
                                  ? semiJoinFilter
                                  : new AndDimFilter(
                                      ImmutableList.of(
                                          semiJoinFilter,
                                          left.getQueryBuilder().getFilter()
                                      )
                                  );

      return left.withQueryBuilder(left.getQueryBuilder().withFilter(newFilter));
    } else {
      return null;
    }
  }
}
