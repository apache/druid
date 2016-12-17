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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.granularity.QueryGranularities;
import io.druid.granularity.QueryGranularity;
import io.druid.java.util.common.ISE;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.topn.DimensionTopNMetricSpec;
import io.druid.query.topn.InvertedTopNMetricSpec;
import io.druid.query.topn.NumericTopNMetricSpec;
import io.druid.query.topn.TopNMetricSpec;
import io.druid.segment.column.Column;
import io.druid.sql.calcite.expression.ExtractionFns;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.table.DruidTable;
import org.apache.calcite.interpreter.Row;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;

public class DruidQueryBuilder
{
  private final DimFilter filter;
  private final SelectProjection selectProjection;
  private final Grouping grouping;
  private final DimFilter having;
  private final DefaultLimitSpec limitSpec;
  private final RelDataType rowType;
  private final List<String> rowOrder;

  private DruidQueryBuilder(
      final DimFilter filter,
      final SelectProjection selectProjection,
      final Grouping grouping,
      final DimFilter having,
      final DefaultLimitSpec limitSpec,
      final RelDataType rowType,
      final List<String> rowOrder
  )
  {
    this.filter = filter;
    this.selectProjection = selectProjection;
    this.grouping = grouping;
    this.having = having;
    this.limitSpec = limitSpec;
    this.rowType = Preconditions.checkNotNull(rowType, "rowType");
    this.rowOrder = Preconditions.checkNotNull(ImmutableList.copyOf(rowOrder), "rowOrder");

    if (selectProjection != null && grouping != null) {
      throw new ISE("Cannot have both selectProjection and grouping");
    }
  }

  public static DruidQueryBuilder fullScan(final DruidTable druidTable, final RelDataTypeFactory relDataTypeFactory)
  {
    final RelDataType rowType = druidTable.getRowType(relDataTypeFactory);
    final List<String> rowOrder = Lists.newArrayListWithCapacity(rowType.getFieldCount());
    for (RelDataTypeField field : rowType.getFieldList()) {
      rowOrder.add(field.getName());
    }
    return new DruidQueryBuilder(null, null, null, null, null, rowType, rowOrder);
  }

  public DruidQueryBuilder withFilter(final DimFilter newFilter)
  {
    Preconditions.checkNotNull(newFilter, "newFilter");
    return new DruidQueryBuilder(newFilter, selectProjection, grouping, having, limitSpec, rowType, rowOrder);
  }

  public DruidQueryBuilder withSelectProjection(final SelectProjection newProjection, final List<String> newRowOrder)
  {
    Preconditions.checkState(selectProjection == null, "cannot project twice");
    Preconditions.checkState(grouping == null, "cannot project after grouping");
    Preconditions.checkNotNull(newProjection, "newProjection");
    Preconditions.checkState(
        newProjection.getProject().getChildExps().size() == newRowOrder.size(),
        "project size[%,d] != rowOrder size[%,d]",
        newProjection.getProject().getChildExps().size(),
        newRowOrder.size()
    );
    return new DruidQueryBuilder(
        filter,
        newProjection,
        grouping,
        having,
        limitSpec,
        newProjection.getProject().getRowType(),
        newRowOrder
    );
  }

  public DruidQueryBuilder withGrouping(
      final Grouping newGrouping,
      final RelDataType newRowType,
      final List<String> newRowOrder
  )
  {
    Preconditions.checkState(grouping == null, "cannot add grouping twice");
    Preconditions.checkState(having == null, "cannot add grouping after having");
    Preconditions.checkState(limitSpec == null, "cannot add grouping after limitSpec");
    Preconditions.checkNotNull(newGrouping, "newGrouping");
    // Set selectProjection to null now that we're grouping. Grouping subsumes select projection.
    return new DruidQueryBuilder(filter, null, newGrouping, having, limitSpec, newRowType, newRowOrder);
  }

  public DruidQueryBuilder withAdjustedGrouping(
      final Grouping newGrouping,
      final RelDataType newRowType,
      final List<String> newRowOrder
  )
  {
    // Like withGrouping, but without any sanity checks. It's assumed that callers will pass something that makes sense.
    // This is used when adjusting the Grouping while pushing down a post-Aggregate Project or Sort.
    Preconditions.checkNotNull(newGrouping, "newGrouping");
    return new DruidQueryBuilder(filter, null, newGrouping, having, limitSpec, newRowType, newRowOrder);
  }

  public DruidQueryBuilder withHaving(final DimFilter newHaving)
  {
    Preconditions.checkState(having == null, "cannot add having twice");
    Preconditions.checkState(limitSpec == null, "cannot add having after limitSpec");
    Preconditions.checkState(grouping != null, "cannot add having before grouping");
    Preconditions.checkNotNull(newHaving, "newHaving");
    return new DruidQueryBuilder(filter, selectProjection, grouping, newHaving, limitSpec, rowType, rowOrder);
  }

  public DruidQueryBuilder withLimitSpec(final DefaultLimitSpec newLimitSpec)
  {
    Preconditions.checkState(limitSpec == null, "cannot add limitSpec twice");
    Preconditions.checkNotNull(newLimitSpec, "newLimitSpec");
    return new DruidQueryBuilder(filter, selectProjection, grouping, having, newLimitSpec, rowType, rowOrder);
  }

  public DimFilter getFilter()
  {
    return filter;
  }

  public SelectProjection getSelectProjection()
  {
    return selectProjection;
  }

  public Grouping getGrouping()
  {
    return grouping;
  }

  public DimFilter getHaving()
  {
    return having;
  }

  public DefaultLimitSpec getLimitSpec()
  {
    return limitSpec;
  }

  public RelDataType getRowType()
  {
    return rowType;
  }

  public List<String> getRowOrder()
  {
    return rowOrder;
  }

  public RelTrait[] getRelTraits()
  {
    final List<RelFieldCollation> collations = Lists.newArrayList();
    if (limitSpec != null) {
      for (OrderByColumnSpec orderBy : limitSpec.getColumns()) {
        final int i = rowOrder.indexOf(orderBy.getDimension());
        final RelFieldCollation.Direction direction = orderBy.getDirection() == OrderByColumnSpec.Direction.ASCENDING
                                                      ? RelFieldCollation.Direction.ASCENDING
                                                      : RelFieldCollation.Direction.DESCENDING;
        collations.add(new RelFieldCollation(i, direction));
      }
    }

    if (!collations.isEmpty()) {
      return new RelTrait[]{RelCollations.of(collations)};
    } else {
      return new RelTrait[]{};
    }
  }

  public void accumulate(
      final DruidTable druidTable,
      final Function<Row, Void> sink
  )
  {
    final PlannerConfig config = druidTable.getPlannerConfig();

    if (grouping == null) {
      QueryMaker.executeSelect(druidTable, this, sink);
    } else if (asQueryGranularityIfTimeseries() != null) {
      QueryMaker.executeTimeseries(druidTable, this, sink);
    } else if (asTopNMetricSpecIfTopN(config.getMaxTopNLimit(), config.isUseApproximateTopN()) != null) {
      QueryMaker.executeTopN(druidTable, this, sink);
    } else {
      QueryMaker.executeGroupBy(druidTable, this, sink);
    }
  }

  /**
   * Determine if this query can be run as a Timeseries query, and if so, return the query granularity.
   *
   * @return query granularity, or null
   */
  public QueryGranularity asQueryGranularityIfTimeseries()
  {
    if (grouping == null) {
      return null;
    }

    final List<DimensionSpec> dimensions = grouping.getDimensions();

    if (dimensions.isEmpty()) {
      return QueryGranularities.ALL;
    } else if (dimensions.size() == 1) {
      final DimensionSpec dimensionSpec = Iterables.getOnlyElement(dimensions);
      final QueryGranularity gran = ExtractionFns.toQueryGranularity(dimensionSpec.getExtractionFn());

      if (gran == null || !dimensionSpec.getDimension().equals(Column.TIME_COLUMN_NAME)) {
        // Timeseries only applies if the single dimension is granular __time.
        return null;
      }

      if (having != null) {
        // Timeseries does not offer HAVING.
        return null;
      }

      // Timeseries only applies if sort is null, or if sort is on the time dimension.
      final boolean sortingOnTime =
          limitSpec == null || limitSpec.getColumns().isEmpty()
          || (limitSpec.getLimit() == Integer.MAX_VALUE
              && limitSpec.getColumns().size() == 1
              && limitSpec.getColumns().get(0).getDimension().equals(dimensionSpec.getOutputName())
              && limitSpec.getColumns().get(0).getDirection() == OrderByColumnSpec.Direction.ASCENDING);

      if (sortingOnTime) {
        return ExtractionFns.toQueryGranularity(dimensionSpec.getExtractionFn());
      }
    }

    return null;
  }

  /**
   * Determine if this query can be run as a topN query, and if so, returns the metric spec for ordering.
   *
   * @param maxTopNLimit       maximum limit to consider for conversion to a topN
   * @param useApproximateTopN true if we should allow approximate topNs, false otherwise
   *
   * @return metric spec, or null
   */
  public TopNMetricSpec asTopNMetricSpecIfTopN(
      final int maxTopNLimit,
      final boolean useApproximateTopN
  )
  {
    // Must have GROUP BY one column, ORDER BY one column, limit less than maxTopNLimit, and no HAVING.
    if (grouping == null
        || grouping.getDimensions().size() != 1
        || limitSpec == null
        || limitSpec.getColumns().size() != 1
        || limitSpec.getLimit() > maxTopNLimit
        || having != null) {
      return null;
    }

    final DimensionSpec dimensionSpec = Iterables.getOnlyElement(grouping.getDimensions());
    final OrderByColumnSpec limitColumn = Iterables.getOnlyElement(limitSpec.getColumns());

    if (limitColumn.getDimension().equals(dimensionSpec.getOutputName())) {
      // DimensionTopNMetricSpec is exact; always return it even if allowApproximate is false.
      final DimensionTopNMetricSpec baseMetricSpec = new DimensionTopNMetricSpec(
          null,
          limitColumn.getDimensionComparator()
      );
      return limitColumn.getDirection() == OrderByColumnSpec.Direction.ASCENDING
             ? baseMetricSpec
             : new InvertedTopNMetricSpec(baseMetricSpec);
    } else if (useApproximateTopN) {
      // ORDER BY metric
      final NumericTopNMetricSpec baseMetricSpec = new NumericTopNMetricSpec(limitColumn.getDimension());
      return limitColumn.getDirection() == OrderByColumnSpec.Direction.ASCENDING
             ? new InvertedTopNMetricSpec(baseMetricSpec)
             : baseMetricSpec;
    } else {
      return null;
    }
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DruidQueryBuilder that = (DruidQueryBuilder) o;

    if (filter != null ? !filter.equals(that.filter) : that.filter != null) {
      return false;
    }
    if (selectProjection != null ? !selectProjection.equals(that.selectProjection) : that.selectProjection != null) {
      return false;
    }
    if (grouping != null ? !grouping.equals(that.grouping) : that.grouping != null) {
      return false;
    }
    if (having != null ? !having.equals(that.having) : that.having != null) {
      return false;
    }
    if (limitSpec != null ? !limitSpec.equals(that.limitSpec) : that.limitSpec != null) {
      return false;
    }
    if (rowType != null ? !rowType.equals(that.rowType) : that.rowType != null) {
      return false;
    }
    return rowOrder != null ? rowOrder.equals(that.rowOrder) : that.rowOrder == null;

  }

  @Override
  public int hashCode()
  {
    int result = filter != null ? filter.hashCode() : 0;
    result = 31 * result + (selectProjection != null ? selectProjection.hashCode() : 0);
    result = 31 * result + (grouping != null ? grouping.hashCode() : 0);
    result = 31 * result + (having != null ? having.hashCode() : 0);
    result = 31 * result + (limitSpec != null ? limitSpec.hashCode() : 0);
    result = 31 * result + (rowType != null ? rowType.hashCode() : 0);
    result = 31 * result + (rowOrder != null ? rowOrder.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "DruidQueryBuilder{" +
           "filter=" + filter +
           ", selectProjection=" + selectProjection +
           ", grouping=" + grouping +
           ", having=" + having +
           ", limitSpec=" + limitSpec +
           ", rowOrder=" + rowOrder +
           '}';
  }
}
