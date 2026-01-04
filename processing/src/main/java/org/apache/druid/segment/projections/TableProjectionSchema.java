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

package org.apache.druid.segment.projections;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.AggregateProjectionMetadata;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class TableProjectionSchema implements BaseTableProjectionSchema
{
  public static final String TYPE_NAME = "base-table";

  public static TableProjectionSchema fromMetadata(List<String> dims, Metadata metadata)
  {
    return new TableProjectionSchema(
        VirtualColumns.create(
            Granularities.toVirtualColumn(metadata.getQueryGranularity(), Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME)
        ),
        dims,
        metadata.getAggregators(),
        metadata.getOrdering()
    );
  }

  private final VirtualColumns virtualColumns;
  private final List<String> columnNames;
  @Nullable
  private final AggregatorFactory[] aggregators;
  private final List<OrderBy> ordering;

  // computed fields
  private final int timeColumnPosition;
  private final Granularity effectiveGranularity;

  @JsonCreator
  public TableProjectionSchema(
      @JsonProperty("virtualColumns") VirtualColumns virtualColumns,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("aggregators") @Nullable AggregatorFactory[] aggregators,
      @JsonProperty("ordering") List<OrderBy> ordering
  )
  {
    if (CollectionUtils.isNullOrEmpty(columns)) {
      throw DruidException.defensive("base table projection schema columns must not be null or empty");
    }
    if (ordering == null) {
      throw DruidException.defensive("base table projection schema ordering must not be null");
    }
    this.virtualColumns = virtualColumns == null ? VirtualColumns.EMPTY : virtualColumns;
    this.columnNames = columns;
    this.aggregators = aggregators == null ? new AggregatorFactory[0] : aggregators;
    this.ordering = ordering;

    int foundTimePosition = -1;
    Granularity granularity = null;
    for (int i = 0; i < ordering.size(); i++) {
      OrderBy orderBy = ordering.get(i);
      if (orderBy.getColumnName().equals(ColumnHolder.TIME_COLUMN_NAME)) {
        foundTimePosition = i;
        // base tables always store granularity virtual column as this name
        final VirtualColumn vc = this.virtualColumns.getVirtualColumn(Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME);
        if (vc != null) {
          granularity = Granularities.fromVirtualColumn(vc);
        } else {
          granularity = Granularities.NONE;
        }
      }
    }
    if (granularity == null) {
      throw DruidException.defensive("base table doesn't have a [%s] column?", ColumnHolder.TIME_COLUMN_NAME);
    }
    this.timeColumnPosition = foundTimePosition;
    this.effectiveGranularity = granularity;
  }

  @JsonIgnore
  @Override
  public List<String> getColumnNames()
  {
    List<String> columns = new ArrayList<>(columnNames.size() + (aggregators != null ? aggregators.length : 0));
    columns.addAll(columnNames);
    for (AggregatorFactory aggregator : aggregators) {
      columns.add(aggregator.getName());
    }
    return columns;
  }

  @JsonProperty
  @Override
  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
  }

  @JsonProperty
  public List<String> getColumns()
  {
    return columnNames;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public AggregatorFactory[] getAggregators()
  {
    return aggregators;
  }

  @JsonIgnore
  @Override
  public int getTimeColumnPosition()
  {
    return timeColumnPosition;
  }

  @JsonIgnore
  @Override
  public Granularity getEffectiveGranularity()
  {
    return effectiveGranularity;
  }

  @JsonProperty
  @Override
  public List<OrderBy> getOrdering()
  {
    return ordering;
  }

  @JsonIgnore
  @Override
  public List<String> getDimensionNames()
  {
    if (timeColumnPosition == 0) {
      return columnNames.subList(1, columnNames.size());
    }
    final List<String> dimsWithoutTime = Lists.newArrayListWithCapacity(columnNames.size() - 1);
    for (String column : columnNames) {
      if (ColumnHolder.TIME_COLUMN_NAME.equals(column)) {
        continue;
      }
      dimsWithoutTime.add(column);
    }
    return dimsWithoutTime;
  }

  @Override
  public Metadata asMetadata(List<AggregateProjectionMetadata> projections)
  {
    return new Metadata(
        null,
        aggregators,
        null,
        effectiveGranularity,
        false,
        ordering,
        projections
    );
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableProjectionSchema that = (TableProjectionSchema) o;
    return Objects.equals(virtualColumns, that.virtualColumns)
           && Objects.equals(columnNames, that.columnNames)
           && Objects.deepEquals(aggregators, that.aggregators)
           && Objects.equals(ordering, that.ordering);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(virtualColumns, columnNames, Arrays.hashCode(aggregators), ordering);
  }

  @Override
  public String toString()
  {
    return "TableProjectionSchema{" +
           "virtualColumns=" + virtualColumns +
           ", columnNames=" + columnNames +
           ", aggregators=" + Arrays.toString(aggregators) +
           ", ordering=" + ordering +
           '}';
  }
}
