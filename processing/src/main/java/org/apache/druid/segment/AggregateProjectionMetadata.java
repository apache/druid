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
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

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
      if (o1.getGranularity().isFinerThan(o2.getGranularity())) {
        return 1;
      }
      if (o2.getGranularity().isFinerThan(o1.getGranularity())) {
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
    private final VirtualColumns virtualColumns;
    private final List<String> groupingColumns;
    private final AggregatorFactory[] aggregators;
    private final List<OrderBy> ordering;
    private final List<OrderBy> orderingWithTimeSubstitution;

    // computed fields
    private final int timeColumnPosition;
    private final Granularity granularity;

    @JsonCreator
    public Schema(
        @JsonProperty("name") String name,
        @JsonProperty("timeColumnName") @Nullable String timeColumnName,
        @JsonProperty("virtualColumns") @Nullable VirtualColumns virtualColumns,
        @JsonProperty("groupingColumns") @Nullable List<String> groupingColumns,
        @JsonProperty("aggregators") @Nullable AggregatorFactory[] aggregators,
        @JsonProperty("ordering") List<OrderBy> ordering
    )
    {
      this.name = name;
      if (CollectionUtils.isNullOrEmpty(groupingColumns) && (aggregators == null || aggregators.length == 0)) {
        throw DruidException.defensive("groupingColumns and aggregators must not both be null or empty");
      }
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
      this.granularity = granularity == null ? Granularities.ALL : granularity;
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
    public Granularity getGranularity()
    {
      return granularity;
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

      if (timeColumnName != null) {
        matchBuilder.remapColumn(timeColumnName, ColumnHolder.TIME_COLUMN_NAME)
                    .addReferencedPhysicalColumn(ColumnHolder.TIME_COLUMN_NAME);
      }
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
                          .addPreAggregatedAggregator(combining);
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
      final VirtualColumn buildSpecVirtualColumn = queryVirtualColumns.getVirtualColumn(column);
      if (buildSpecVirtualColumn != null) {
        // check to see if we have an equivalent virtual column defined in the projection, if so we can
        final VirtualColumn projectionEquivalent = virtualColumns.findEquivalent(buildSpecVirtualColumn);
        if (projectionEquivalent != null) {
          if (!buildSpecVirtualColumn.getOutputName().equals(projectionEquivalent.getOutputName())) {
            matchBuilder.remapColumn(
                buildSpecVirtualColumn.getOutputName(),
                projectionEquivalent.getOutputName()
            );
          }
          return matchBuilder.addReferencedPhysicalColumn(projectionEquivalent.getOutputName());
        }

        matchBuilder.addReferenceedVirtualColumn(buildSpecVirtualColumn);
        final List<String> requiredInputs = buildSpecVirtualColumn.requiredColumns();
        if (requiredInputs.size() == 1 && ColumnHolder.TIME_COLUMN_NAME.equals(requiredInputs.get(0))) {
          // special handle time granularity. in the future this should be reworked to push this concept into the
          // virtual column and underlying expression itself, but this will do for now
          final Granularity virtualGranularity = Granularities.fromVirtualColumn(buildSpecVirtualColumn);
          if (virtualGranularity != null) {
            if (virtualGranularity.isFinerThan(granularity)) {
              return null;
            }
            return matchBuilder.remapColumn(column, ColumnHolder.TIME_COLUMN_NAME)
                               .addReferencedPhysicalColumn(ColumnHolder.TIME_COLUMN_NAME);
          } else {
            // anything else with __time requires none granularity
            if (Granularities.NONE.equals(granularity)) {
              return matchBuilder;
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
          return matchBuilder.addReferencedPhysicalColumn(column);
        }
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
      Schema schema = (Schema) o;
      return Objects.equals(name, schema.name)
             && Objects.equals(timeColumnName, schema.timeColumnName)
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
          virtualColumns,
          groupingColumns,
          Arrays.hashCode(aggregators),
          ordering
      );
    }
  }
}
