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
import com.google.common.collect.Lists;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonTypeName(AggregateProjectionSpec.TYPE_NAME)
public class AggregateProjectionMetadata
{
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
    this.schema = schema;
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
    private final List<String> groupingColumns;
    private final VirtualColumns virtualColumns;
    private final AggregatorFactory[] aggregators;
    private final List<OrderBy> ordering;
    private final List<OrderBy> orderingWithTimeSubstitution;

    // computed fields
    @Nullable
    private final String timeColumnName;
    private final int timeColumnPosition;
    private final Granularity granularity;

    @JsonCreator
    public Schema(
        @JsonProperty("name") String name,
        @JsonProperty("virtualColumns") @Nullable VirtualColumns virtualColumns,
        @JsonProperty("groupingColumns") List<String> groupingColumns,
        @JsonProperty("aggregators") @Nullable AggregatorFactory[] aggregators,
        @JsonProperty("ordering") List<OrderBy> ordering,
        @JsonProperty("timeColumnName") @Nullable String timeColumnName
    )
    {
      this.name = name;
      if (CollectionUtils.isNullOrEmpty(groupingColumns)) {
        throw InvalidInput.exception("groupingColumns must not be null or empty");
      }
      this.virtualColumns = virtualColumns == null ? VirtualColumns.EMPTY : virtualColumns;
      this.groupingColumns = groupingColumns;
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

    @JsonProperty
    @Nullable
    public String getTimeColumnName()
    {
      return timeColumnName;
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
     * Check if this projection "matches" a {@link CursorBuildSpec}.
     *
     * @param queryCursorBuildSpec      {@link CursorBuildSpec} the contains the required inputs to build a
     *                                  {@link org.apache.druid.segment.CursorHolder}
     * @param referencedVirtualColumns  collection of {@link VirtualColumn} from the queryCursorBuildSpec which are still
     *                                  required after matching the projection. The projection may contain pre-computed
     *                                  {@link VirtualColumn} which will not be included in this set since they are
     *                                  physical columns on the projection
     * @param remapColumns              Mapping of {@link CursorBuildSpec} column names to projection column names, used
     *                                  to allow column selector factories to provide projection column selectors as the
     *                                  names the queryCursorBuildSpec needs
     * @param physicalColumnChecker     {@link Projections.PhysicalColumnChecker} which can determine if a physical column
     *                                  required by queryCursorBuildSpec is available on the projection OR does not exist
     *                                  on the base table either
     * @return true if the projection can be used to correctly satisfy the queryCursorBuildSpec, populating
     *         referencedVirtualColumns and remapColumns by side-effect
     */
    public boolean matches(
        CursorBuildSpec queryCursorBuildSpec,
        Set<VirtualColumn> referencedVirtualColumns,
        Map<String, String> remapColumns,
        Projections.PhysicalColumnChecker physicalColumnChecker
    )
    {
      if (!queryCursorBuildSpec.isCompatibleOrdering(orderingWithTimeSubstitution)) {
        return false;
      }
      final List<String> queryGrouping = queryCursorBuildSpec.getGroupingColumns();
      if (queryGrouping != null) {
        for (String queryColumn : queryGrouping) {
          if (!hasRequiredColumn(
              queryColumn,
              queryCursorBuildSpec.getVirtualColumns(),
              referencedVirtualColumns,
              remapColumns,
              physicalColumnChecker
          )) {
            return false;
          }
        }
      }
      if (queryCursorBuildSpec.getFilter() != null) {
        for (String queryColumn : queryCursorBuildSpec.getFilter().getRequiredColumns()) {
          if (!hasRequiredColumn(
              queryColumn,
              queryCursorBuildSpec.getVirtualColumns(),
              referencedVirtualColumns,
              remapColumns,
              physicalColumnChecker
          )) {
            return false;
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
              // todo (clint): bubble this back up to the modified build spec
              remapColumns.put(queryAgg.getName(), projectionAgg.getName());
              foundMatch = true;
            }
          }
          allMatch = allMatch && foundMatch;
        }
        return allMatch;
      }
      return true;
    }


    /**
     * Ensure that the projection has the specified column required by a {@link CursorBuildSpec} in one form or another.
     * If the column is a {@link VirtualColumn} on the build spec, ensure that the projection has an equivalent virtual
     * column, or has the required inputs to compute the virtual column. If an equivalent virtual column exists, its
     * name will be added to the 'mapBuildSpecColumnToProjectionColumn' parameter so that the build spec name can be
     * mapped to the projection name. If no equivalent virtual column exists, but the inputs are available on the
     * projection to compute it, it will be added to 'requiredBuildSpecVirtualColumns' parameter.
     * <p>
     * If the column is not a virtual column, {@link Projections.PhysicalColumnChecker} is a helper method which returns
     * true if the column is present on the projection OR if the column is NOT present on the base table (meaning missing
     * columns that do not exist anywhere do not disqualify a projection from being used).
     */
    private boolean hasRequiredColumn(
        String column,
        VirtualColumns buildSpecVirtualColumns,
        Set<VirtualColumn> requiredBuildSpecVirtualColumns,
        Map<String, String> mapBuildSpecColumnToProjectionColumn,
        Projections.PhysicalColumnChecker physicalColumnChecker
    )
    {
      final VirtualColumn buildSpecVirtualColumn = buildSpecVirtualColumns.getVirtualColumn(column);
      if (buildSpecVirtualColumn != null) {
        // check to see if we have an equivalent virtual column defined in the projection, if so we can
        final VirtualColumn projectionEquivalent = virtualColumns.findEquivalent(buildSpecVirtualColumn);
        if (projectionEquivalent != null) {
          if (!buildSpecVirtualColumn.getOutputName().equals(projectionEquivalent.getOutputName())) {
            mapBuildSpecColumnToProjectionColumn.put(
                buildSpecVirtualColumn.getOutputName(),
                projectionEquivalent.getOutputName()
            );
          }
          return true;
        }

        requiredBuildSpecVirtualColumns.add(buildSpecVirtualColumn);
        final List<String> requiredInputs = buildSpecVirtualColumn.requiredColumns();
        if (requiredInputs.size() == 1 && ColumnHolder.TIME_COLUMN_NAME.equals(requiredInputs.get(0))) {
          // wtb some sort of virtual column comparison function that can check if projection granularity time column
          // satisifies query granularity virtual column
          // can rebind? q.canRebind("__time", p)
          // special handle time granularity
          final Granularity virtualGranularity = Granularities.fromVirtualColumn(buildSpecVirtualColumn);
          if (virtualGranularity != null) {
            if (virtualGranularity.isFinerThan(granularity)) {
              return false;
            }
            mapBuildSpecColumnToProjectionColumn.put(column, timeColumnName);
            return true;
          } else {
            // anything else with __time requires none granularity
            return Granularities.NONE.equals(granularity);
          }
        } else {
          for (String required : requiredInputs) {
            if (!hasRequiredColumn(
                required,
                buildSpecVirtualColumns,
                requiredBuildSpecVirtualColumns,
                mapBuildSpecColumnToProjectionColumn,
                physicalColumnChecker
            )) {
              return false;
            }
          }
        }
        return true;
      } else {
        // a physical column must either be present or also missing from the base table
        return physicalColumnChecker.check(name, column);
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
      return Objects.equals(name, schema.name) && Objects.equals(
          groupingColumns,
          schema.groupingColumns
      ) && Objects.equals(virtualColumns, schema.virtualColumns) && Objects.deepEquals(
          aggregators,
          schema.aggregators
      ) && Objects.equals(ordering, schema.ordering);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(name, groupingColumns, virtualColumns, Arrays.hashCode(aggregators), ordering);
    }
  }
}
