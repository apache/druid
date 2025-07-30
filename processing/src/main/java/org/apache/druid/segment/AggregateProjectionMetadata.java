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

package org.apache.druid.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.IsBooleanFilter;
import org.apache.druid.segment.filter.TrueFilter;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Aggregate projection schema and row count information to store in {@link Metadata} which itself is stored inside a
 * segment, defining which projections exist for the segment.
 * <p>
 * Decorated with {@link JsonTypeInfo} annotations as a future-proofing mechanism in the event we add other types of
 * projections and need to extract out a base interface from this class.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonTypeName(AggregateProjectionSpec.TYPE_NAME)
public class AggregateProjectionMetadata
{
  private static final Interner<Schema> SCHEMA_INTERNER = Interners.newWeakInterner();


  public static final Comparator<AggregateProjectionMetadata> COMPARATOR = (o1, o2) -> {
    int rowCompare = Integer.compare(o1.numRows, o2.numRows);
    if (rowCompare != 0) {
      return rowCompare;
    }
    return Schema.COMPARATOR.compare(o1.getSchema(), o2.getSchema());
  };

  private final Schema schema;
  private final int numRows;

  @JsonCreator
  public AggregateProjectionMetadata(
      @JsonProperty("schema") Schema schema,
      @JsonProperty("numRows") int numRows
  )
  {
    this.schema = SCHEMA_INTERNER.intern(schema);
    this.numRows = numRows;
  }

  @JsonProperty
  public Schema getSchema()
  {
    return schema;
  }

  @JsonProperty
  public int getNumRows()
  {
    return numRows;
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
    AggregateProjectionMetadata that = (AggregateProjectionMetadata) o;
    return numRows == that.numRows && Objects.equals(schema, that.schema);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(schema, numRows);
  }

  @Override
  public String toString()
  {
    return "AggregateProjectionMetadata{" +
           "schema=" + schema +
           ", numRows=" + numRows +
           '}';
  }

  public static class Schema
  {
    /**
     * It is not likely the best way to find the best matching projections, but it is the one we have for now. This
     * comparator is used to sort all the projections in a segment "best" first, where best is defined as fewest grouping
     * columns, most virtual columns and aggregators, as an approximation of likely to have the fewest number of rows to
     * scan.
     */
    public static final Comparator<Schema> COMPARATOR = (o1, o2) -> {
      // coarsest granularity first
      if (o1.getEffectiveGranularity().isFinerThan(o2.getEffectiveGranularity())) {
        return 1;
      }
      if (o2.getEffectiveGranularity().isFinerThan(o1.getEffectiveGranularity())) {
        return -1;
      }
      // fewer dimensions first
      final int dimsCompare = Integer.compare(
          o1.groupingColumns.size(),
          o2.groupingColumns.size()
      );
      if (dimsCompare != 0) {
        return dimsCompare;
      }
      // more metrics first
      int metCompare = Integer.compare(o2.aggregators.length, o1.aggregators.length);
      if (metCompare != 0) {
        return metCompare;
      }
      // more virtual columns first
      final int virtCompare = Integer.compare(
          o2.virtualColumns.getVirtualColumns().length,
          o1.virtualColumns.getVirtualColumns().length
      );
      if (virtCompare != 0) {
        return virtCompare;
      }
      return o1.name.compareTo(o2.name);
    };

    private final String name;
    @Nullable
    private final String timeColumnName;
    @Nullable
    private final DimFilter filter;
    private final VirtualColumns virtualColumns;
    private final List<String> groupingColumns;
    private final AggregatorFactory[] aggregators;
    private final List<OrderBy> ordering;
    private final List<OrderBy> orderingWithTimeSubstitution;

    // computed fields
    private final int timeColumnPosition;
    private final Granularity effectiveGranularity;

    @JsonCreator
    public Schema(
        @JsonProperty("name") String name,
        @JsonProperty("timeColumnName") @Nullable String timeColumnName,
        @JsonProperty("filter") @Nullable DimFilter filter,
        @JsonProperty("virtualColumns") @Nullable VirtualColumns virtualColumns,
        @JsonProperty("groupingColumns") @Nullable List<String> groupingColumns,
        @JsonProperty("aggregators") @Nullable AggregatorFactory[] aggregators,
        @JsonProperty("ordering") List<OrderBy> ordering
    )
    {
      if (name == null || name.isEmpty()) {
        throw DruidException.defensive("projection schema name cannot be null or empty");
      }
      this.name = name;
      if (CollectionUtils.isNullOrEmpty(groupingColumns) && (aggregators == null || aggregators.length == 0)) {
        throw DruidException.defensive(
            "projection schema[%s] groupingColumns and aggregators must not both be null or empty",
            name
        );
      }
      if (ordering == null) {
        throw DruidException.defensive("projection schema[%s] ordering must not be null", name);
      }
      this.filter = filter;
      this.virtualColumns = virtualColumns == null ? VirtualColumns.EMPTY : virtualColumns;
      this.groupingColumns = groupingColumns == null ? Collections.emptyList() : groupingColumns;
      this.aggregators = aggregators == null ? new AggregatorFactory[0] : aggregators;
      this.ordering = ordering;

      int foundTimePosition = -1;
      this.orderingWithTimeSubstitution = Lists.newArrayListWithCapacity(ordering.size());
      Granularity granularity = null;
      for (int i = 0; i < ordering.size(); i++) {
        OrderBy orderBy = ordering.get(i);
        if (orderBy.getColumnName().equals(timeColumnName)) {
          orderingWithTimeSubstitution.add(new OrderBy(ColumnHolder.TIME_COLUMN_NAME, orderBy.getOrder()));
          foundTimePosition = i;
          timeColumnName = groupingColumns.get(foundTimePosition);
          final VirtualColumn vc = this.virtualColumns.getVirtualColumn(groupingColumns.get(foundTimePosition));
          if (vc != null) {
            granularity = Granularities.fromVirtualColumn(vc);
          } else {
            granularity = Granularities.NONE;
          }
        } else {
          orderingWithTimeSubstitution.add(orderBy);
        }
      }
      this.timeColumnName = timeColumnName;
      this.timeColumnPosition = foundTimePosition;
      this.effectiveGranularity = granularity == null ? Granularities.ALL : granularity;
    }

    @JsonProperty
    public String getName()
    {
      return name;
    }

    @JsonProperty
    @Nullable
    public String getTimeColumnName()
    {
      return timeColumnName;
    }

    @JsonProperty
    @Nullable
    public DimFilter getFilter()
    {
      return filter;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public VirtualColumns getVirtualColumns()
    {
      return virtualColumns;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public List<String> getGroupingColumns()
    {
      return groupingColumns;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public AggregatorFactory[] getAggregators()
    {
      return aggregators;
    }

    @JsonProperty
    public List<OrderBy> getOrdering()
    {
      return ordering;
    }

    @JsonIgnore
    public List<OrderBy> getOrderingWithTimeColumnSubstitution()
    {
      return orderingWithTimeSubstitution;
    }

    @JsonIgnore
    public int getTimeColumnPosition()
    {
      return timeColumnPosition;
    }

    @JsonIgnore
    public Granularity getEffectiveGranularity()
    {
      return effectiveGranularity;
    }

    /**
     * Check if this projection "matches" a {@link CursorBuildSpec} for a query to see if we can use a projection
     * instead. For a projection to match, all grouping columns of the build spec must match, virtual columns of the
     * build spec must either be available as a physical column on the projection, or the inputs to the virtual column
     * must be available on the projection, and all aggregators must be compatible with pre-aggregated columns of the
     * projection per {@link AggregatorFactory#substituteCombiningFactory(AggregatorFactory)}. If the projection
     * matches, this method returns a {@link Projections.ProjectionMatch} which contains an updated
     * {@link CursorBuildSpec} which has the remaining virtual columns from the original build spec which must still be
     * computed and the 'combining' aggregator factories to process the pre-aggregated data from the projection, as well
     * as a mapping of query column names to projection column names.
     *
     * @param queryCursorBuildSpec  the {@link CursorBuildSpec} that contains the required inputs to build a
     *                              {@link CursorHolder} for a query
     * @param physicalColumnChecker Helper utility which can determine if a physical column required by
     *                              queryCursorBuildSpec is available on the projection OR does not exist on the base
     *                              table either
     * @return a {@link Projections.ProjectionMatch} if the {@link CursorBuildSpec} matches the projection, which
     * contains information such as which
     */
    @Nullable
    public Projections.ProjectionMatch matches(
        CursorBuildSpec queryCursorBuildSpec,
        Projections.PhysicalColumnChecker physicalColumnChecker
    )
    {
      if (!queryCursorBuildSpec.isCompatibleOrdering(orderingWithTimeSubstitution)) {
        return null;
      }
      Projections.ProjectionMatchBuilder matchBuilder = new Projections.ProjectionMatchBuilder();

      final List<String> queryGrouping = queryCursorBuildSpec.getGroupingColumns();
      if (queryGrouping != null) {
        for (String queryColumn : queryGrouping) {
          matchBuilder = matchRequiredColumn(
              matchBuilder,
              queryColumn,
              queryCursorBuildSpec.getVirtualColumns(),
              physicalColumnChecker
          );
          if (matchBuilder == null) {
            return null;
          }
          // a query grouping column must also be defined as a projection grouping column
          if (isInvalidGrouping(queryColumn) || isInvalidGrouping(matchBuilder.getRemapValue(queryColumn))) {
            return null;
          }
        }
      }
      if (queryCursorBuildSpec.getFilter() != null) {
        for (String queryColumn : queryCursorBuildSpec.getFilter().getRequiredColumns()) {
          matchBuilder = matchRequiredColumn(
              matchBuilder,
              queryColumn,
              queryCursorBuildSpec.getVirtualColumns(),
              physicalColumnChecker
          );
          if (matchBuilder == null) {
            return null;
          }
        }
      }
      if (!CollectionUtils.isNullOrEmpty(queryCursorBuildSpec.getAggregators())) {
        boolean allMatch = true;
        for (AggregatorFactory queryAgg : queryCursorBuildSpec.getAggregators()) {
          boolean foundMatch = false;
          for (AggregatorFactory projectionAgg : aggregators) {
            final AggregatorFactory combining = queryAgg.substituteCombiningFactory(projectionAgg);
            if (combining != null) {
              matchBuilder.remapColumn(queryAgg.getName(), projectionAgg.getName())
                          .addReferencedPhysicalColumn(projectionAgg.getName())
                          .addPreAggregatedAggregator(combining)
                          .addMatchedQueryColumns(queryAgg.requiredFields());
              foundMatch = true;
              break;
            }
          }
          allMatch = allMatch && foundMatch;
        }
        if (!allMatch) {
          return null;
        }
      }
      // validate physical and virtual columns have all been accounted for
      final Set<String> matchedQueryColumns = matchBuilder.getMatchedQueryColumns();
      if (queryCursorBuildSpec.getPhysicalColumns() != null) {
        for (String queryColumn : queryCursorBuildSpec.getPhysicalColumns()) {
          // time is special handled
          if (ColumnHolder.TIME_COLUMN_NAME.equals(queryColumn)) {
            continue;
          }
          if (!matchedQueryColumns.contains(queryColumn)) {
            matchBuilder = matchRequiredColumn(
                matchBuilder,
                queryColumn,
                queryCursorBuildSpec.getVirtualColumns(),
                physicalColumnChecker
            );
            if (matchBuilder == null) {
              return null;
            }
          }
        }
        for (VirtualColumn vc : queryCursorBuildSpec.getVirtualColumns().getVirtualColumns()) {
          if (!matchedQueryColumns.contains(vc.getOutputName())) {
            matchBuilder = matchRequiredColumn(
                matchBuilder,
                vc.getOutputName(),
                queryCursorBuildSpec.getVirtualColumns(),
                physicalColumnChecker
            );
            if (matchBuilder == null) {
              return null;
            }
          }
        }
      }

      // if the projection has a filter, the query must contain this filter match
      if (filter != null) {
        final Filter queryFilter = queryCursorBuildSpec.getFilter();
        if (queryFilter != null) {
          // try to rewrite the query filter into a projection filter, if the rewrite is valid, we can proceed
          final Filter projectionFilter = filter.toOptimizedFilter(false);
          final Map<String, String> filterRewrites = new HashMap<>();
          // start with identity
          for (String required : queryFilter.getRequiredColumns()) {
            filterRewrites.put(required, required);
          }
          // overlay projection rewrites
          filterRewrites.putAll(matchBuilder.getRemapColumns());

          final Filter remappedQueryFilter = queryFilter.rewriteRequiredColumns(filterRewrites);

          final Filter rewritten = rewriteFilter(projectionFilter, remappedQueryFilter);
          // if the filter does not contain the projection filter, we cannot match this projection
          if (rewritten == null) {
            return null;
          }
          //noinspection ObjectEquality
          if (rewritten == ProjectionFilterMatch.INSTANCE) {
            // we can remove the whole thing since the query filter exactly matches the projection filter
            matchBuilder.rewriteFilter(null);
          } else {
            // otherwise, we partially rewrote the query filter to eliminate the projection filter since it is baked in
            matchBuilder.rewriteFilter(rewritten);
          }
        } else {
          // projection has a filter, but the query doesn't, no good
          return null;
        }
      } else {
        // projection doesn't have a filter, retain the original
        matchBuilder.rewriteFilter(queryCursorBuildSpec.getFilter());
      }

      return matchBuilder.build(queryCursorBuildSpec);
    }

    /**
     * Ensure that the projection has the specified column required by a {@link CursorBuildSpec} in one form or another.
     * If the column is a {@link VirtualColumn} on the build spec, ensure that the projection has an equivalent virtual
     * column, or has the required inputs to compute the virtual column. If an equivalent virtual column exists, its
     * name will be added to {@link Projections.ProjectionMatchBuilder#remapColumn(String, String)} so the query
     * virtual column name can be mapped to the projection physical column name. If no equivalent virtual column exists,
     * but the inputs are available on the projection to compute it, it will be added to
     * {@link Projections.ProjectionMatchBuilder#addReferenceedVirtualColumn(VirtualColumn)}.
     * <p>
     * Finally, if the column is not a virtual column in the query, it is checked with
     * {@link Projections.PhysicalColumnChecker} which true if the column is present on the projection OR if the column
     * is NOT present on the base table (meaning missing columns that do not exist anywhere do not disqualify a
     * projection from being used).
     *
     * @param matchBuilder          match state to add mappings of query virtual columns to projection physical columns
     *                              and query virtual columns which still must be computed from projection physical
     *                              columns
     * @param column                Column name to check
     * @param queryVirtualColumns   {@link VirtualColumns} from the {@link CursorBuildSpec} required by the query
     * @param physicalColumnChecker Helper to check if the physical column exists on a projection, or does not exist on
     *                              the base table
     * @return {@link Projections.ProjectionMatchBuilder} with updated state per the rules described above, or null
     * if the column cannot be matched
     */
    @Nullable
    private Projections.ProjectionMatchBuilder matchRequiredColumn(
        Projections.ProjectionMatchBuilder matchBuilder,
        String column,
        VirtualColumns queryVirtualColumns,
        Projections.PhysicalColumnChecker physicalColumnChecker
    )
    {
      final VirtualColumn queryVirtualColumn = queryVirtualColumns.getVirtualColumn(column);
      if (queryVirtualColumn != null) {
        matchBuilder.addMatchedQueryColumn(column)
                    .addMatchedQueryColumns(queryVirtualColumn.requiredColumns());
        // check to see if we have an equivalent virtual column defined in the projection, if so we can
        final VirtualColumn projectionEquivalent = virtualColumns.findEquivalent(queryVirtualColumn);
        if (projectionEquivalent != null) {
          final String remapColumnName;
          if (Objects.equals(projectionEquivalent.getOutputName(), timeColumnName)) {
            remapColumnName = ColumnHolder.TIME_COLUMN_NAME;
          } else {
            remapColumnName = projectionEquivalent.getOutputName();
          }
          if (!queryVirtualColumn.getOutputName().equals(remapColumnName)) {
            matchBuilder.remapColumn(queryVirtualColumn.getOutputName(), remapColumnName);
          }
          return matchBuilder.addReferencedPhysicalColumn(remapColumnName);
        }

        matchBuilder.addReferenceedVirtualColumn(queryVirtualColumn);
        final List<String> requiredInputs = queryVirtualColumn.requiredColumns();
        if (requiredInputs.size() == 1 && ColumnHolder.TIME_COLUMN_NAME.equals(requiredInputs.get(0))) {
          // special handle time granularity. in the future this should be reworked to push this concept into the
          // virtual column and underlying expression itself, but this will do for now
          final Granularity virtualGranularity = Granularities.fromVirtualColumn(queryVirtualColumn);
          if (virtualGranularity != null) {
            if (virtualGranularity.isFinerThan(effectiveGranularity)) {
              return null;
            }
            // same granularity, replace virtual column directly by remapping it to the physical column
            if (effectiveGranularity.equals(virtualGranularity)) {
              return matchBuilder.remapColumn(column, ColumnHolder.TIME_COLUMN_NAME)
                                 .addReferencedPhysicalColumn(ColumnHolder.TIME_COLUMN_NAME);
            }
            return matchBuilder.addReferencedPhysicalColumn(ColumnHolder.TIME_COLUMN_NAME);
          } else {
            // anything else with __time requires none granularity
            if (Granularities.NONE.equals(effectiveGranularity)) {
              return matchBuilder.addReferencedPhysicalColumn(ColumnHolder.TIME_COLUMN_NAME);
            }
            return null;
          }
        } else {
          for (String required : requiredInputs) {
            matchBuilder = matchRequiredColumn(
                matchBuilder,
                required,
                queryVirtualColumns,
                physicalColumnChecker
            );
            if (matchBuilder == null) {
              return null;
            }
          }
          return matchBuilder;
        }
      } else {
        if (physicalColumnChecker.check(name, column)) {
          return matchBuilder.addMatchedQueryColumn(column)
                             .addReferencedPhysicalColumn(column);
        }
        return null;
      }
    }

    /**
     * Check if a column is either part of {@link #groupingColumns}, or at least is not present in
     * {@link #virtualColumns}. Naively we would just check that grouping column contains the column in question,
     * however we can also use a projection when a column is truly missing. {@link #matchRequiredColumn} returns a
     * match builder if the column is present as either a physical column, or a virtual column, but a virtual column
     * could also be present for an aggregator input, so we must further check that a column not in the grouping list
     * is also not a virtual column, the implication being that it is a missing column.
     */
    private boolean isInvalidGrouping(@Nullable String columnName)
    {
      if (columnName == null) {
        return false;
      }
      return !groupingColumns.contains(columnName) && virtualColumns.exists(columnName);
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
      Schema schema = (Schema) o;
      return Objects.equals(name, schema.name)
             && Objects.equals(timeColumnName, schema.timeColumnName)
             && Objects.equals(filter, schema.filter)
             && Objects.equals(virtualColumns, schema.virtualColumns)
             && Objects.equals(groupingColumns, schema.groupingColumns)
             && Objects.deepEquals(aggregators, schema.aggregators)
             && Objects.equals(ordering, schema.ordering);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(
          name,
          timeColumnName,
          filter,
          virtualColumns,
          groupingColumns,
          Arrays.hashCode(aggregators),
          ordering
      );
    }

    @Override
    public String toString()
    {
      return "Schema{" +
             "name='" + name + '\'' +
             ", timeColumnName='" + timeColumnName + '\'' +
             ", virtualColumns=" + virtualColumns +
             ", groupingColumns=" + groupingColumns +
             ", aggregators=" + Arrays.toString(aggregators) +
             ", ordering=" + ordering +
             ", timeColumnPosition=" + timeColumnPosition +
             ", effectiveGranularity=" + effectiveGranularity +
             ", orderingWithTimeSubstitution=" + orderingWithTimeSubstitution +
             '}';
    }
  }

  /**
   * Rewrites a query {@link Filter} if possible, removing the {@link Filter} of a projection. To match a projection
   * filter, the query filter must be equal to the projection filter, or must contain the projection filter as the child
   * of an AND filter. This method returns null
   * indicating that a rewrite is impossible with the implication that the query cannot use the projection because the
   * projection doesn't contain all the rows the query would match if not using the projection.
   */
  @Nullable
  public static Filter rewriteFilter(@Nullable Filter projectionFilter, @Nullable Filter queryFilter)
  {
    if (projectionFilter == null || queryFilter == null) {
      return queryFilter;
    }
    if (queryFilter.equals(projectionFilter)) {
      return ProjectionFilterMatch.INSTANCE;
    }
    if (queryFilter instanceof IsBooleanFilter && ((IsBooleanFilter) queryFilter).isTrue()) {
      final IsBooleanFilter  isTrueFilter = (IsBooleanFilter) queryFilter;
      final Filter rewritten = rewriteFilter(projectionFilter, isTrueFilter.getBaseFilter());
      if (rewritten == null) {
        return null;
      }
      //noinspection ObjectEquality
      if (rewritten == ProjectionFilterMatch.INSTANCE) {
        return ProjectionFilterMatch.INSTANCE;
      }
      return new IsBooleanFilter(rewritten, true);
    }
    if (queryFilter instanceof AndFilter) {
      AndFilter andFilter = (AndFilter) queryFilter;
      List<Filter> newChildren = Lists.newArrayListWithExpectedSize(andFilter.getFilters().size());
      boolean childRewritten = false;
      for (Filter filter : andFilter.getFilters()) {
        Filter rewritten = rewriteFilter(projectionFilter, filter);
        //noinspection ObjectEquality
        if (rewritten == ProjectionFilterMatch.INSTANCE) {
          childRewritten = true;
        } else {
          if (rewritten != null) {
            newChildren.add(rewritten);
            childRewritten = true;
          } else {
            newChildren.add(filter);
          }
        }
      }
      // at least one child must have been rewritten to rewrite the AND
      if (childRewritten) {
        if (newChildren.size() > 1) {
          return new AndFilter(newChildren);
        } else {
          return newChildren.get(0);
        }
      }
      return null;
    }
    return null;
  }

  static final class ProjectionFilterMatch extends TrueFilter
  {
    private static final ProjectionFilterMatch INSTANCE = new ProjectionFilterMatch();
  }
}
