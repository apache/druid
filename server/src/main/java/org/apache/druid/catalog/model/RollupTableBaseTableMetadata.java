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

package org.apache.druid.catalog.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.RollupTableProjectionSpec;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Catalog layout metadata for {@link RollupTableProjectionSpec} base tables. The catalog column list declares the
 * output schema of the rollup: the columns named by an entry of {@link #aggregators} are the metric columns, every
 * other declared column is a grouping column, and, mirroring the physical layout, the grouping columns must be
 * declared before the metric columns. {@link #createSpec(List)} combines the declared columns with this metadata into
 * the physical spec used to generate segments; as with the other layouts, the optional {@link #virtualColumns} carry
 * only the query granularity, and the optional {@link #columnSchemas} customize the {@link DimensionSchema} used for a
 * declared grouping column (a metric column's physical representation is fixed by its aggregator).
 */
@JsonTypeName(RollupTableBaseTableMetadata.TYPE_NAME)
public class RollupTableBaseTableMetadata implements DatasourceBaseTableMetadata
{
  public static final String TYPE_NAME = RollupTableProjectionSpec.TYPE_NAME;

  private final VirtualColumns virtualColumns;
  private final AggregatorFactory[] aggregators;
  private final List<DimensionSchema> columnSchemas;

  @JsonCreator
  public RollupTableBaseTableMetadata(
      @JsonProperty("virtualColumns") @Nullable VirtualColumns virtualColumns,
      @JsonProperty("aggregators") @Nullable AggregatorFactory[] aggregators,
      @JsonProperty("columnSchemas") @Nullable List<DimensionSchema> columnSchemas
  )
  {
    this.virtualColumns = virtualColumns == null ? VirtualColumns.EMPTY : virtualColumns;
    this.aggregators = aggregators == null ? new AggregatorFactory[0] : aggregators;
    this.columnSchemas = columnSchemas == null ? Collections.emptyList() : columnSchemas;
  }

  @Override
  @JsonProperty("type")
  public String getType()
  {
    return TYPE_NAME;
  }

  @Override
  @JsonProperty("virtualColumns")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
  }

  /**
   * The aggregators computed over the rows collapsed into each grouping tuple. Each names the declared metric column
   * it fills.
   */
  @JsonProperty("aggregators")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public AggregatorFactory[] getAggregators()
  {
    return aggregators;
  }

  /**
   * Per-column customizations of the {@link DimensionSchema} used during segment creation, keyed by
   * {@link DimensionSchema#getName()}; empty when every declared grouping column uses the schema derived from its
   * declared type. These do not define columns: every entry must customize a declared grouping column.
   */
  @JsonProperty("columnSchemas")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<DimensionSchema> getColumnSchemas()
  {
    return columnSchemas;
  }

  /**
   * Creates the physical spec from the declared catalog columns: a declared column named by an aggregator is a metric
   * column, whose declared type must match what its aggregator produces; every other declared column is a grouping
   * column, derived like the other layouts derive theirs. Declared column order is the physical segment order, so the
   * grouping columns must be declared before the metric columns; the metric columns' declared order becomes the
   * aggregator order of the spec.
   */
  @Override
  public RollupTableProjectionSpec createSpec(List<ColumnSpec> columns)
  {
    if (CollectionUtils.isNullOrEmpty(columns)) {
      throw InvalidInput.exception(
          "Cannot define a [%s] base table without declared columns; the catalog column list defines the table schema",
          TYPE_NAME
      );
    }
    final Map<String, AggregatorFactory> aggregatorsByName = new HashMap<>();
    for (AggregatorFactory aggregator : aggregators) {
      if (aggregator == null) {
        throw InvalidInput.exception("aggregators must not contain null entries");
      }
      if (aggregatorsByName.put(aggregator.getName(), aggregator) != null) {
        throw InvalidInput.exception("aggregators contains duplicate entries for column [%s]", aggregator.getName());
      }
    }
    final Map<String, DimensionSchema> customSchemas = BaseTableColumns.indexColumnSchemas(columnSchemas);

    final Set<String> declaredNames = new HashSet<>();
    final List<DimensionSchema> groupingColumns = new ArrayList<>(columns.size());
    final List<AggregatorFactory> orderedAggregators = new ArrayList<>(aggregators.length);
    for (ColumnSpec column : columns) {
      declaredNames.add(column.name());
      final AggregatorFactory aggregator = aggregatorsByName.get(column.name());
      if (aggregator == null) {
        if (!orderedAggregators.isEmpty()) {
          throw InvalidInput.exception(
              "grouping column [%s] is declared after metric column [%s]; the declared order is the physical segment"
              + " order, so grouping columns must be declared before the metric columns",
              column.name(),
              orderedAggregators.get(orderedAggregators.size() - 1).getName()
          );
        }
        groupingColumns.add(BaseTableColumns.toDimensionSchema(column, customSchemas.get(column.name()), TYPE_NAME));
      } else {
        final ColumnType declaredType = Columns.druidType(column);
        final ColumnType producedType = aggregator.getIntermediateType();
        if (declaredType == null || !declaredType.equals(producedType)) {
          throw InvalidInput.exception(
              "metric column [%s] is declared as type [%s], but its aggregator produces [%s]; the declared type of a"
              + " metric column must match what its aggregator stores",
              column.name(),
              column.dataType(),
              producedType
          );
        }
        if (customSchemas.containsKey(column.name())) {
          throw InvalidInput.exception(
              "columnSchemas cannot customize metric column [%s]: the physical representation of a metric column is"
              + " fixed by its aggregator",
              column.name()
          );
        }
        orderedAggregators.add(aggregator);
      }
    }
    for (String aggregatorName : aggregatorsByName.keySet()) {
      if (!declaredNames.contains(aggregatorName)) {
        throw InvalidInput.exception(
            "aggregator [%s] does not fill a declared column; every aggregator produces a metric column, so declare"
            + " [%s] in the table's column list",
            aggregatorName,
            aggregatorName
        );
      }
    }
    for (String customized : customSchemas.keySet()) {
      if (!declaredNames.contains(customized)) {
        throw InvalidInput.exception(
            "columnSchemas entry [%s] does not customize a declared column; column schemas do not define columns,"
            + " declare [%s] in the table's column list",
            customized,
            customized
        );
      }
    }
    return RollupTableProjectionSpec.builder()
                                    .virtualColumns(virtualColumns)
                                    .groupingColumns(groupingColumns)
                                    .aggregators(orderedAggregators.toArray(new AggregatorFactory[0]))
                                    .build();
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RollupTableBaseTableMetadata that = (RollupTableBaseTableMetadata) o;
    return Objects.equals(virtualColumns, that.virtualColumns)
           && Arrays.equals(aggregators, that.aggregators)
           && Objects.equals(columnSchemas, that.columnSchemas);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(virtualColumns, Arrays.hashCode(aggregators), columnSchemas);
  }

  @Override
  public String toString()
  {
    return "RollupTableBaseTableMetadata{" +
           "virtualColumns=" + virtualColumns +
           ", aggregators=" + Arrays.toString(aggregators) +
           ", columnSchemas=" + columnSchemas +
           '}';
  }
}
