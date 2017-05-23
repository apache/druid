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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.QueryDataSource;
import io.druid.query.ResourceLimitExceededException;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.segment.VirtualColumn;
import io.druid.segment.virtual.ExtractionFnVirtualColumn;
import io.druid.sql.calcite.expression.SimpleExtraction;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;
import java.util.Set;

public class DruidSemiJoin extends DruidRel<DruidSemiJoin>
{
  private final DruidRel<?> left;
  private final DruidRel<?> right;
  private final List<SimpleExtraction> leftSimpleExtractions;
  private final List<Integer> rightKeys;
  private final int maxSemiJoinRowsInMemory;

  private DruidSemiJoin(
      final RelOptCluster cluster,
      final RelTraitSet traitSet,
      final DruidRel left,
      final DruidRel right,
      final List<SimpleExtraction> leftSimpleExtractions,
      final List<Integer> rightKeys,
      final int maxSemiJoinRowsInMemory
  )
  {
    super(cluster, traitSet, left.getQueryMaker());
    this.left = left;
    this.right = right;
    this.leftSimpleExtractions = ImmutableList.copyOf(leftSimpleExtractions);
    this.rightKeys = ImmutableList.copyOf(rightKeys);
    this.maxSemiJoinRowsInMemory = maxSemiJoinRowsInMemory;
  }

  public static DruidSemiJoin from(
      final DruidRel left,
      final DruidRel right,
      final List<Integer> leftKeys,
      final List<Integer> rightKeys,
      final PlannerConfig plannerConfig
  )
  {
    final ImmutableList.Builder<SimpleExtraction> listBuilder = ImmutableList.builder();
    for (Integer key : leftKeys) {
      final String columnName = left.getQueryBuilder().getRowOrder().get(key);

      final VirtualColumn leftVirtualColumn = left.getQueryBuilder().getVirtualColumnRegistry().get(columnName);
      if (leftVirtualColumn != null) {
        // VirtualColumns not allowed to remain in "left" since we have no way of forcing later rules to include them.
        // See if we can get rid of this virtual column reference, otherwise give up.
        if (leftVirtualColumn instanceof ExtractionFnVirtualColumn) {
          final ExtractionFnVirtualColumn extractionColumn = (ExtractionFnVirtualColumn) leftVirtualColumn;
          if (left.getSourceRowSignature().getColumnType(extractionColumn.getFieldName()) != null) {
            listBuilder.add(SimpleExtraction.of(extractionColumn.getFieldName(), extractionColumn.getExtractionFn()));
          } else {
            return null;
          }
        } else {
          return null;
        }
      } else {
        listBuilder.add(SimpleExtraction.of(columnName, null));
      }
    }

    return new DruidSemiJoin(
        left.getCluster(),
        left.getTraitSet(),
        left,
        right,
        listBuilder.build(),
        rightKeys,
        plannerConfig.getMaxSemiJoinRowsInMemory()
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
        leftSimpleExtractions,
        rightKeys,
        maxSemiJoinRowsInMemory
    );
  }

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
        right,
        leftSimpleExtractions,
        rightKeys,
        maxSemiJoinRowsInMemory
    );
  }

  @Override
  public DruidSemiJoin asDruidConvention()
  {
    return new DruidSemiJoin(
        getCluster(),
        getTraitSet().replace(DruidConvention.instance()),
        left,
        right,
        leftSimpleExtractions,
        rightKeys,
        maxSemiJoinRowsInMemory
    );
  }

  @Override
  public int getQueryCount()
  {
    return left.getQueryCount() + right.getQueryCount();
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
  public RelWriter explainTerms(RelWriter pw)
  {
    return pw
        .item("leftSimpleExtractions", leftSimpleExtractions)
        .item("leftQuery", left.getQueryBuilder())
        .item("rightKeys", rightKeys)
        .item("rightQuery", right.getQueryBuilder());
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
    // Build list of acceptable values from right side.
    final Set<List<String>> valuess = Sets.newHashSet();
    final List<DimFilter> filters = Lists.newArrayList();
    right.runQuery().accumulate(
        null,
        new Accumulator<Object, Object[]>()
        {
          @Override
          public Object accumulate(final Object dummyValue, final Object[] row)
          {
            final List<String> values = Lists.newArrayListWithCapacity(rightKeys.size());

            for (int i : rightKeys) {
              final Object value = row[i];
              final String stringValue = value != null ? String.valueOf(value) : "";
              values.add(stringValue);
              if (values.size() > maxSemiJoinRowsInMemory) {
                throw new ResourceLimitExceededException(
                    String.format("maxSemiJoinRowsInMemory[%,d] exceeded", maxSemiJoinRowsInMemory)
                );
              }
            }

            if (valuess.add(values)) {
              final List<DimFilter> bounds = Lists.newArrayList();
              for (int i = 0; i < values.size(); i++) {
                bounds.add(
                    new BoundDimFilter(
                        leftSimpleExtractions.get(i).getColumn(),
                        values.get(i),
                        values.get(i),
                        false,
                        false,
                        null,
                        leftSimpleExtractions.get(i).getExtractionFn(),
                        getSourceRowSignature().naturalStringComparator(leftSimpleExtractions.get(i))
                    )
                );
              }
              filters.add(new AndDimFilter(bounds));
            }
            return null;
          }
        }
    );

    valuess.clear();

    if (!filters.isEmpty()) {
      // Add a filter to the left side. Use OR of singleton Bound filters so they can be simplified later.
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
