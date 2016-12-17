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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.query.ResourceLimitExceededException;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.sql.calcite.aggregation.Aggregation;
import io.druid.sql.calcite.expression.RowExtraction;
import io.druid.sql.calcite.table.DruidTable;
import io.druid.sql.calcite.table.DruidTables;
import org.apache.calcite.DataContext;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.Row;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.Set;

public class DruidSemiJoin extends DruidRel<DruidSemiJoin>
{
  private final SemiJoin semiJoin;
  private final DruidRel left;
  private final DruidRel right;
  private final RexNode condition;
  private final List<RowExtraction> leftRowExtractions;
  private final List<Integer> rightKeys;
  private final int maxSemiJoinRowsInMemory;

  private DruidSemiJoin(
      final RelOptCluster cluster,
      final RelTraitSet traitSet,
      final SemiJoin semiJoin,
      final DruidRel left,
      final DruidRel right,
      final RexNode condition,
      final List<RowExtraction> leftRowExtractions,
      final List<Integer> rightKeys,
      final int maxSemiJoinRowsInMemory
  )
  {
    super(cluster, traitSet);
    this.semiJoin = semiJoin;
    this.left = left;
    this.right = right;
    this.condition = condition;
    this.leftRowExtractions = ImmutableList.copyOf(leftRowExtractions);
    this.rightKeys = ImmutableList.copyOf(rightKeys);
    this.maxSemiJoinRowsInMemory = maxSemiJoinRowsInMemory;
  }

  public static DruidSemiJoin from(
      final SemiJoin semiJoin,
      final RelTraitSet traitSet,
      final DruidRel left,
      final DruidRel right
  )
  {
    if (semiJoin.getLeftKeys().size() != semiJoin.getRightKeys().size()) {
      throw new ISE("WTF?! SemiJoin with different left/right key count?");
    }

    final ImmutableList.Builder<RowExtraction> listBuilder = ImmutableList.builder();
    for (Integer key : semiJoin.getLeftKeys()) {
      final RowExtraction rex = RowExtraction.fromQueryBuilder(left.getQueryBuilder(), key);
      if (rex == null) {
        // Can't figure out what to filter the left-hand side on...
        return null;
      }
      listBuilder.add(rex);
    }

    return new DruidSemiJoin(
        semiJoin.getCluster(),
        traitSet,
        semiJoin,
        left,
        right,
        semiJoin.getCondition(),
        listBuilder.build(),
        semiJoin.getRightKeys(),
        right.getDruidTable().getPlannerConfig().getMaxSemiJoinRowsInMemory()
    );
  }

  @Override
  public Class<Object[]> getElementType()
  {
    return Object[].class;
  }

  @Override
  public DruidTable getDruidTable()
  {
    return left.getDruidTable();
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
        semiJoin,
        left.withQueryBuilder(newQueryBuilder),
        right,
        condition,
        leftRowExtractions,
        rightKeys,
        maxSemiJoinRowsInMemory
    );
  }

  @Override
  public DruidSemiJoin asBindable()
  {
    return new DruidSemiJoin(
        getCluster(),
        getTraitSet().plus(BindableConvention.INSTANCE),
        semiJoin,
        left,
        right,
        condition,
        leftRowExtractions,
        rightKeys,
        maxSemiJoinRowsInMemory
    );
  }

  @Override
  public void accumulate(final Function<Row, Void> sink)
  {
    final Pair<DruidQueryBuilder, List<Integer>> pair = getRightQueryBuilderWithGrouping();
    final DruidQueryBuilder rightQueryBuilderAdjusted = pair.lhs;
    final List<Integer> rightKeysAdjusted = pair.rhs;

    // Build list of acceptable values from right side.
    final Set<List<String>> valuess = Sets.newHashSet();
    final List<DimFilter> filters = Lists.newArrayList();
    rightQueryBuilderAdjusted.accumulate(
        right.getDruidTable(),
        new Function<Row, Void>()
        {
          @Override
          public Void apply(final Row row)
          {
            final List<String> values = Lists.newArrayListWithCapacity(rightKeysAdjusted.size());

            for (int i : rightKeysAdjusted) {
              final Object value = row.getObject(i);
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
                        leftRowExtractions.get(i).getColumn(),
                        values.get(i),
                        values.get(i),
                        false,
                        false,
                        null,
                        leftRowExtractions.get(i).getExtractionFn(),
                        DruidTables.naturalStringComparator(getDruidTable(), leftRowExtractions.get(i))
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

      left.getQueryBuilder().withFilter(newFilter).accumulate(
          left.getDruidTable(),
          sink
      );
    }
  }

  @Override
  public Enumerable<Object[]> bind(final DataContext dataContext)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  protected RelDataType deriveRowType()
  {
    return left.getRowType();
  }

  @Override
  public RelWriter explainTerms(RelWriter pw)
  {
    final Pair<DruidQueryBuilder, List<Integer>> rightQueryBuilderWithGrouping = getRightQueryBuilderWithGrouping();
    return pw
        .item("leftDataSource", left.getDruidTable().getDataSource())
        .item("leftRowExtractions", leftRowExtractions)
        .item("leftQuery", left.getQueryBuilder())
        .item("rightDataSource", right.getDruidTable().getDataSource())
        .item("rightKeysAdjusted", rightQueryBuilderWithGrouping.rhs)
        .item("rightQuery", rightQueryBuilderWithGrouping.lhs);
  }

  @Override
  public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq)
  {
    return semiJoin.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  private Pair<DruidQueryBuilder, List<Integer>> getRightQueryBuilderWithGrouping()
  {
    if (right.getQueryBuilder().getGrouping() != null) {
      return Pair.of(right.getQueryBuilder(), rightKeys);
    } else {
      // Add grouping on the join key to limit resultset from data nodes.
      final List<DimensionSpec> dimensionSpecs = Lists.newArrayList();
      final List<RelDataType> rowTypes = Lists.newArrayList();
      final List<String> rowOrder = Lists.newArrayList();
      final List<Integer> rightKeysAdjusted = Lists.newArrayList();

      int counter = 0;
      for (final int key : rightKeys) {
        final String keyDimensionOutputName = "v" + key;
        final RowExtraction rex = RowExtraction.fromQueryBuilder(right.getQueryBuilder(), key);
        if (rex == null) {
          throw new ISE("WTF?! Can't find dimensionSpec to group on!");
        }

        final DimensionSpec dimensionSpec = rex.toDimensionSpec(left.getDruidTable(), keyDimensionOutputName);
        if (dimensionSpec == null) {
          throw new ISE("WTF?! Can't translate row expression to dimensionSpec: %s", rex);
        }

        dimensionSpecs.add(dimensionSpec);
        rowTypes.add(right.getQueryBuilder().getRowType().getFieldList().get(key).getType());
        rowOrder.add(dimensionSpec.getOutputName());
        rightKeysAdjusted.add(counter++);
      }

      final DruidQueryBuilder newQueryBuilder = right
          .getQueryBuilder()
          .withGrouping(
              Grouping.create(dimensionSpecs, ImmutableList.<Aggregation>of()),
              getCluster().getTypeFactory().createStructType(rowTypes, rowOrder),
              rowOrder
          );

      return Pair.of(newQueryBuilder, rightKeysAdjusted);
    }
  }
}
