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
import org.apache.druid.data.input.impl.ClusteredValueGroupsBaseTableProjectionSpec;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.segment.NestedDataColumnSchema;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Catalog layout metadata for {@link ClusteredValueGroupsBaseTableProjectionSpec} base tables. Declares the
 * {@link #clusteringColumns} (names of declared catalog columns that rows are clustered by) and optional
 * {@link #virtualColumns} that compute stored columns at ingest time; everything else about the physical spec
 * (column names, types, and order) is taken from the catalog column list by {@link #createSpec(List)}. The declared
 * column order is the physical segment order, so, mirroring the physical spec, the clustering columns must be
 * declared as the leading prefix of the column list.
 */
@JsonTypeName(ClusteredValueGroupsBaseTableMetadata.TYPE_NAME)
public class ClusteredValueGroupsBaseTableMetadata implements DatasourceBaseTableMetadata
{
  public static final String TYPE_NAME = ClusteredValueGroupsBaseTableProjectionSpec.TYPE_NAME;

  private final List<String> clusteringColumns;
  private final VirtualColumns virtualColumns;

  @JsonCreator
  public ClusteredValueGroupsBaseTableMetadata(
      @JsonProperty("clusteringColumns") List<String> clusteringColumns,
      @JsonProperty("virtualColumns") @Nullable VirtualColumns virtualColumns
  )
  {
    this.clusteringColumns = clusteringColumns == null ? Collections.emptyList() : clusteringColumns;
    this.virtualColumns = virtualColumns == null ? VirtualColumns.EMPTY : virtualColumns;
  }

  @Override
  @JsonProperty("type")
  public String getType()
  {
    return TYPE_NAME;
  }

  @JsonProperty("clusteringColumns")
  public List<String> getClusteringColumns()
  {
    return clusteringColumns;
  }

  @Override
  @JsonProperty("virtualColumns")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
  }

  /**
   * Creates the physical spec from the declared catalog columns, used verbatim: the declared column order is the
   * physical segment order, so the clustering columns must be declared as the leading prefix of the column list (in
   * {@link #clusteringColumns} order); anything else is a validation error. Every clustering column must be a declared
   * column, a clustering column computed by a virtual column at ingest time is still a stored, queryable column, so it
   * too must appear in the column list. All ordering and layout rules are enforced by the spec itself.
   */
  @Override
  public ClusteredValueGroupsBaseTableProjectionSpec createSpec(List<ColumnSpec> columns)
  {
    if (CollectionUtils.isNullOrEmpty(columns)) {
      throw InvalidInput.exception(
          "Cannot define a [%s] base table without declared columns; the catalog column list defines the table schema",
          TYPE_NAME
      );
    }
    final Set<String> declaredNames = new HashSet<>();
    final List<DimensionSchema> specColumns = new ArrayList<>(columns.size());
    for (ColumnSpec column : columns) {
      declaredNames.add(column.name());
      specColumns.add(toDimensionSchema(column));
    }
    for (String clusteringColumn : clusteringColumns) {
      if (!declaredNames.contains(clusteringColumn)) {
        throw InvalidInput.exception(
            "clustering column [%s] is not a declared column; clustering columns must be declared as the leading"
            + " prefix of the table's column list, including columns computed by a virtual column at ingest time"
            + " (they are stored columns)",
            clusteringColumn
        );
      }
    }
    return ClusteredValueGroupsBaseTableProjectionSpec.builder()
                                                      .virtualColumns(virtualColumns)
                                                      .columns(specColumns)
                                                      .clusteringColumns(clusteringColumns)
                                                      .build();
  }

  private static DimensionSchema toDimensionSchema(ColumnSpec column)
  {
    ColumnType druidType = Columns.druidType(column);
    if (druidType == null) {
      druidType = ColumnType.STRING;
    }
    if (druidType.isPrimitive() || druidType.isPrimitiveArray()) {
      // The declared type is retained in the ingestion schema (primitive arrays are cast, rather than left to an
      // untyped auto column whose type is inferred from the ingested values; note that the auto schema stores
      // FLOAT ARRAY as DOUBLE ARRAY).
      return DimensionSchema.getDefaultSchemaForBuiltInType(column.name(), druidType);
    }
    if (ColumnType.NESTED_DATA.equals(druidType)) {
      return new NestedDataColumnSchema(column.name(), NestedDataColumnSchema.DEFAULT_FORMAT_VERSION);
    }
    // Other complex types cannot be ingested into a clustered base table: there is no dimension handler for them,
    // and clustered base tables have no aggregators to produce them.
    throw InvalidInput.exception(
        "column [%s] has unsupported type [%s] for a clustered base table; supported types are primitive, primitive"
        + " array, and COMPLEX<json> columns",
        column.name(),
        druidType
    );
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClusteredValueGroupsBaseTableMetadata that = (ClusteredValueGroupsBaseTableMetadata) o;
    return Objects.equals(clusteringColumns, that.clusteringColumns)
           && Objects.equals(virtualColumns, that.virtualColumns);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(clusteringColumns, virtualColumns);
  }

  @Override
  public String toString()
  {
    return "ClusteredValueGroupsBaseTableMetadata{" +
           "clusteringColumns=" + clusteringColumns +
           ", virtualColumns=" + virtualColumns +
           '}';
  }
}
