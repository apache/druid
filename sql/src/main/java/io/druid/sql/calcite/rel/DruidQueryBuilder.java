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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.granularity.QueryGranularities;
import io.druid.granularity.QueryGranularity;
import io.druid.java.util.common.ISE;
import io.druid.query.DataSource;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.having.DimFilterHavingSpec;
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.select.PagingSpec;
import io.druid.query.select.SelectQuery;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.topn.DimensionTopNMetricSpec;
import io.druid.query.topn.InvertedTopNMetricSpec;
import io.druid.query.topn.NumericTopNMetricSpec;
import io.druid.query.topn.TopNMetricSpec;
import io.druid.query.topn.TopNQuery;
import io.druid.segment.column.Column;
import io.druid.segment.column.ValueType;
import io.druid.sql.calcite.expression.ExtractionFns;
import io.druid.sql.calcite.filtration.Filtration;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.Map;

public class DruidQueryBuilder
{
  private final DimFilter filter;
  private final SelectProjection selectProjection;
  private final Grouping grouping;
  private final DimFilter having;
  private final DefaultLimitSpec limitSpec;
  private final RelDataType rowType;
  private final RowSignature outputRowSignature;

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

    if (selectProjection != null && grouping != null) {
      throw new ISE("Cannot have both selectProjection and grouping");
    }

    final RowSignature.Builder rowSignatureBuilder = RowSignature.builder();

    for (int i = 0; i < rowOrder.size(); i++) {
      final RelDataTypeField field = rowType.getFieldList().get(i);
      final SqlTypeName sqlTypeName = field.getType().getSqlTypeName();
      final ValueType valueType;

      if (SqlTypeName.APPROX_TYPES.contains(sqlTypeName)) {
        valueType = ValueType.FLOAT;
      } else if (SqlTypeName.DATETIME_TYPES.contains(sqlTypeName) || SqlTypeName.EXACT_TYPES.contains(sqlTypeName)) {
        valueType = ValueType.LONG;
      } else if (SqlTypeName.CHAR_TYPES.contains(sqlTypeName)) {
        valueType = ValueType.STRING;
      } else if (SqlTypeName.OTHER == sqlTypeName) {
        valueType = ValueType.COMPLEX;
      } else {
        throw new ISE("Cannot translate sqlTypeName[%s] to Druid type for field[%s]", sqlTypeName, rowOrder.get(i));
      }

      rowSignatureBuilder.add(rowOrder.get(i), valueType);
    }

    this.outputRowSignature = rowSignatureBuilder.build();
  }

  public static DruidQueryBuilder fullScan(final RowSignature rowSignature, final RelDataTypeFactory relDataTypeFactory)
  {
    final RelDataType rowType = rowSignature.getRelDataType(relDataTypeFactory);
    final List<String> rowOrder = rowSignature.getRowOrder();
    return new DruidQueryBuilder(null, null, null, null, null, rowType, rowOrder);
  }

  public DruidQueryBuilder withFilter(final DimFilter newFilter)
  {
    return new DruidQueryBuilder(
        newFilter,
        selectProjection,
        grouping,
        having,
        limitSpec,
        rowType,
        outputRowSignature.getRowOrder()
    );
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
    return new DruidQueryBuilder(
        filter,
        selectProjection,
        grouping,
        newHaving,
        limitSpec,
        rowType,
        outputRowSignature.getRowOrder()
    );
  }

  public DruidQueryBuilder withLimitSpec(final DefaultLimitSpec newLimitSpec)
  {
    Preconditions.checkState(limitSpec == null, "cannot add limitSpec twice");
    Preconditions.checkNotNull(newLimitSpec, "newLimitSpec");
    return new DruidQueryBuilder(
        filter,
        selectProjection,
        grouping,
        having,
        newLimitSpec,
        rowType,
        outputRowSignature.getRowOrder()
    );
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

  /**
   * Returns the output row signature for this DruidQueryBuilder. Useful if we will go on to do something else with
   * the rows, like make a QueryDataSource or use a HavingSpec. Note that the field names may be different from the
   * field names returned by {@link #getRowType()}, although positionally they will line up.
   *
   * @return Druid row signature
   */
  public RowSignature getOutputRowSignature()
  {
    return outputRowSignature;
  }

  public RelDataType getRowType()
  {
    return rowType;
  }

  public List<String> getRowOrder()
  {
    return outputRowSignature.getRowOrder();
  }

  public RelTrait[] getRelTraits()
  {
    final List<RelFieldCollation> collations = Lists.newArrayList();
    if (limitSpec != null) {
      for (OrderByColumnSpec orderBy : limitSpec.getColumns()) {
        final int i = outputRowSignature.getRowOrder().indexOf(orderBy.getDimension());
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

  /**
   * Return this query as a Timeseries query, or null if this query is not compatible with Timeseries.
   *
   * @param dataSource         data source to query
   * @param sourceRowSignature row signature of the dataSource
   *
   * @return query or null
   */
  public TimeseriesQuery toTimeseriesQuery(
      final DataSource dataSource,
      final RowSignature sourceRowSignature
  )
  {
    if (grouping == null || having != null) {
      return null;
    }

    final QueryGranularity queryGranularity;
    final List<DimensionSpec> dimensions = grouping.getDimensions();

    if (dimensions.isEmpty()) {
      queryGranularity = QueryGranularities.ALL;
    } else if (dimensions.size() == 1) {
      final DimensionSpec dimensionSpec = Iterables.getOnlyElement(dimensions);
      final QueryGranularity gran = ExtractionFns.toQueryGranularity(dimensionSpec.getExtractionFn());

      if (gran == null || !dimensionSpec.getDimension().equals(Column.TIME_COLUMN_NAME)) {
        // Timeseries only applies if the single dimension is granular __time.
        return null;
      }

      // Timeseries only applies if sort is null, or if the first sort field is the time dimension.
      final boolean sortingOnTime =
          limitSpec == null || limitSpec.getColumns().isEmpty()
          || (limitSpec.getLimit() == Integer.MAX_VALUE
              && limitSpec.getColumns().get(0).getDimension().equals(dimensionSpec.getOutputName()));

      if (sortingOnTime) {
        queryGranularity = gran;
      } else {
        return null;
      }
    } else {
      return null;
    }

    final Filtration filtration = Filtration.create(filter).optimize(sourceRowSignature);

    final boolean descending;
    if (limitSpec != null && !limitSpec.getColumns().isEmpty()) {
      descending = limitSpec.getColumns().get(0).getDirection() == OrderByColumnSpec.Direction.DESCENDING;
    } else {
      descending = false;
    }

    final Map<String, Object> context = Maps.newHashMap();
    context.put("skipEmptyBuckets", true);

    return new TimeseriesQuery(
        dataSource,
        filtration.getQuerySegmentSpec(),
        descending,
        filtration.getDimFilter(),
        queryGranularity,
        grouping.getAggregatorFactories(),
        grouping.getPostAggregators(),
        context
    );
  }

  /**
   * Return this query as a TopN query, or null if this query is not compatible with TopN.
   *
   * @param dataSource         data source to query
   * @param sourceRowSignature row signature of the dataSource
   * @param maxTopNLimit       maxTopNLimit from a PlannerConfig
   * @param useApproximateTopN from a PlannerConfig
   *
   * @return query or null
   */
  public TopNQuery toTopNQuery(
      final DataSource dataSource,
      final RowSignature sourceRowSignature,
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
    final TopNMetricSpec topNMetricSpec;

    if (limitColumn.getDimension().equals(dimensionSpec.getOutputName())) {
      // DimensionTopNMetricSpec is exact; always return it even if allowApproximate is false.
      final DimensionTopNMetricSpec baseMetricSpec = new DimensionTopNMetricSpec(
          null,
          limitColumn.getDimensionComparator()
      );
      topNMetricSpec = limitColumn.getDirection() == OrderByColumnSpec.Direction.ASCENDING
                       ? baseMetricSpec
                       : new InvertedTopNMetricSpec(baseMetricSpec);
    } else if (useApproximateTopN) {
      // ORDER BY metric
      final NumericTopNMetricSpec baseMetricSpec = new NumericTopNMetricSpec(limitColumn.getDimension());
      topNMetricSpec = limitColumn.getDirection() == OrderByColumnSpec.Direction.ASCENDING
                       ? new InvertedTopNMetricSpec(baseMetricSpec)
                       : baseMetricSpec;
    } else {
      return null;
    }

    final Filtration filtration = Filtration.create(filter).optimize(sourceRowSignature);

    return new TopNQuery(
        dataSource,
        Iterables.getOnlyElement(grouping.getDimensions()),
        topNMetricSpec,
        limitSpec.getLimit(),
        filtration.getQuerySegmentSpec(),
        filtration.getDimFilter(),
        QueryGranularities.ALL,
        grouping.getAggregatorFactories(),
        grouping.getPostAggregators(),
        null
    );
  }

  /**
   * Return this query as a GroupBy query, or null if this query is not compatible with GroupBy.
   *
   * @param dataSource         data source to query
   * @param sourceRowSignature row signature of the dataSource
   *
   * @return query or null
   */
  public GroupByQuery toGroupByQuery(
      final DataSource dataSource,
      final RowSignature sourceRowSignature
  )
  {
    if (grouping == null) {
      return null;
    }

    final Filtration filtration = Filtration.create(filter).optimize(sourceRowSignature);

    return new GroupByQuery(
        dataSource,
        filtration.getQuerySegmentSpec(),
        filtration.getDimFilter(),
        QueryGranularities.ALL,
        grouping.getDimensions(),
        grouping.getAggregatorFactories(),
        grouping.getPostAggregators(),
        having != null ? new DimFilterHavingSpec(having) : null,
        limitSpec,
        null
    );
  }

  /**
   * Return this query as a Select query, or null if this query is not compatible with Select.
   *
   * @param dataSource         data source to query
   * @param sourceRowSignature row signature of the dataSource
   *
   * @return query or null
   */
  public SelectQuery toSelectQuery(
      final DataSource dataSource,
      final RowSignature sourceRowSignature
  )
  {
    if (grouping != null) {
      return null;
    }

    final Filtration filtration = Filtration.create(filter).optimize(sourceRowSignature);
    final boolean descending;

    if (limitSpec != null) {
      // Safe to assume limitSpec has zero or one entry; DruidSelectSortRule wouldn't push in anything else.
      if (limitSpec.getColumns().size() > 0) {
        final OrderByColumnSpec orderBy = Iterables.getOnlyElement(limitSpec.getColumns());
        if (!orderBy.getDimension().equals(Column.TIME_COLUMN_NAME)) {
          throw new ISE("WTF?! Got select with non-time orderBy[%s]", orderBy);
        }
        descending = orderBy.getDirection() == OrderByColumnSpec.Direction.DESCENDING;
      } else {
        descending = false;
      }
    } else {
      descending = false;
    }

    return new SelectQuery(
        dataSource,
        filtration.getQuerySegmentSpec(),
        descending,
        filtration.getDimFilter(),
        QueryGranularities.ALL,
        selectProjection != null ? selectProjection.getDimensions() : ImmutableList.<DimensionSpec>of(),
        selectProjection != null ? selectProjection.getMetrics() : ImmutableList.<String>of(),
        null,
        new PagingSpec(null, 0) /* dummy -- will be replaced */,
        null
    );
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
    return outputRowSignature != null
           ? outputRowSignature.equals(that.outputRowSignature)
           : that.outputRowSignature == null;

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
    result = 31 * result + (outputRowSignature != null ? outputRowSignature.hashCode() : 0);
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
           ", outputRowSignature=" + outputRowSignature +
           '}';
  }
}
