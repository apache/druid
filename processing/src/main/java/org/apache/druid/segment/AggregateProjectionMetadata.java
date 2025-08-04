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
import org.apache.druid.segment.column.ColumnHolder;
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
     * Check if a column is either part of {@link #groupingColumns}, or at least is not present in
     * {@link #virtualColumns}. Naively, we would just check that grouping column contains the column in question,
     * however, we can also use a projection when a column is truly missing.
     * {@link org.apache.druid.segment.projections.Projections#matchAggregateProjection} returns a match builder if the
     * column is present as either a physical column, or a virtual column, but a virtual column could also be present
     * for an aggregator input, so we must further check that a column not in the grouping list is also not a virtual
     * column, the implication being that it is a missing column.
     */
    public boolean isInvalidGrouping(@Nullable String columnName)
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
}
