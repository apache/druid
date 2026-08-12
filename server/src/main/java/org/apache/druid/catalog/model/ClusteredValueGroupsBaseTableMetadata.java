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
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Catalog layout metadata for {@link ClusteredValueGroupsBaseTableProjectionSpec} base tables. Declares the
 * {@link #clusteringColumns} (names of declared catalog columns that rows are clustered by) and optional
 * {@link #virtualColumns} that compute stored columns at ingest time; everything else about the physical spec
 * (column names, types, and order) is taken from the catalog column list by {@link #createSpec(List)}. The declared
 * column order is the physical segment order, so, mirroring the physical spec, the clustering columns must be
 * declared as the leading prefix of the column list.
 * <p>
 * The optional {@link #columnSchemas} do NOT define columns, the catalog column list remains the logical schema,
 * they are per-column customizations of the exact {@link DimensionSchema} used to create segments for a declared
 * column, replacing the default schema derived from the declared column type.
 */
@JsonTypeName(ClusteredValueGroupsBaseTableMetadata.TYPE_NAME)
public class ClusteredValueGroupsBaseTableMetadata implements DatasourceBaseTableMetadata
{
  public static final String TYPE_NAME = ClusteredValueGroupsBaseTableProjectionSpec.TYPE_NAME;

  private final List<String> clusteringColumns;
  private final VirtualColumns virtualColumns;
  private final List<DimensionSchema> columnSchemas;

  @JsonCreator
  public ClusteredValueGroupsBaseTableMetadata(
      @JsonProperty("clusteringColumns") List<String> clusteringColumns,
      @JsonProperty("virtualColumns") @Nullable VirtualColumns virtualColumns,
      @JsonProperty("columnSchemas") @Nullable List<DimensionSchema> columnSchemas
  )
  {
    this.clusteringColumns = clusteringColumns == null ? Collections.emptyList() : clusteringColumns;
    this.virtualColumns = virtualColumns == null ? VirtualColumns.EMPTY : virtualColumns;
    this.columnSchemas = columnSchemas == null ? Collections.emptyList() : columnSchemas;
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
   * Per-column customizations of the {@link DimensionSchema} used during segment creation, keyed by
   * {@link DimensionSchema#getName()}; empty when every declared column uses the schema derived from its declared
   * type. These do not define columns: every entry must customize a declared, non-clustering column.
   */
  @JsonProperty("columnSchemas")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<DimensionSchema> getColumnSchemas()
  {
    return columnSchemas;
  }

  /**
   * Creates the physical spec from the declared catalog columns, used verbatim: the declared column order is the
   * physical segment order, so the clustering columns must be declared as the leading prefix of the column list (in
   * {@link #clusteringColumns} order); anything else is a validation error. Every clustering column must be a declared
   * column, a clustering column computed by a virtual column at ingest time is still a stored, queryable column, so it
   * too must appear in the column list. All ordering and layout rules are enforced by the spec itself.
   * <p>
   * A column with an entry in {@link #columnSchemas} uses that {@link DimensionSchema} verbatim in place of the
   * default derived from its declared type. Customizations may only target declared, non-clustering columns (a
   * clustering column's physical representation is fixed by the clustered format, and {@code __time} is always a
   * long), and the schema's type must match the declared logical type so the physical schema cannot silently
   * contradict the SQL schema.
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
    final Map<String, DimensionSchema> customSchemas = indexColumnSchemas();
    final Set<String> declaredNames = new HashSet<>();
    final List<DimensionSchema> specColumns = new ArrayList<>(columns.size());
    for (ColumnSpec column : columns) {
      declaredNames.add(column.name());
      specColumns.add(toDimensionSchema(column, customSchemas.get(column.name())));
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
    return ClusteredValueGroupsBaseTableProjectionSpec.builder()
                                                      .virtualColumns(virtualColumns)
                                                      .columns(specColumns)
                                                      .clusteringColumns(clusteringColumns)
                                                      .build();
  }

  private Map<String, DimensionSchema> indexColumnSchemas()
  {
    final Map<String, DimensionSchema> customSchemas = new HashMap<>();
    for (DimensionSchema schema : columnSchemas) {
      if (schema == null) {
        throw InvalidInput.exception("columnSchemas must not contain null entries");
      }
      if (customSchemas.put(schema.getName(), schema) != null) {
        throw InvalidInput.exception("columnSchemas contains duplicate entries for column [%s]", schema.getName());
      }
    }
    return customSchemas;
  }

  private DimensionSchema toDimensionSchema(ColumnSpec column, @Nullable DimensionSchema customSchema)
  {
    ColumnType druidType = Columns.druidType(column);
    if (druidType == null) {
      // A column declared without a type defaults to STRING (mirroring Columns.convertSignature), but a declared
      // type that does not parse must be rejected rather than silently defaulted: the declared type is the physical
      // segment schema here.
      if (column.dataType() != null) {
        throw InvalidInput.exception(
            "column [%s] has an unrecognized type [%s]; declare a SQL type (such as [%s]) or a Druid type string"
            + " (such as [%s] or [%s])",
            column.name(),
            column.dataType(),
            Columns.SQL_BIGINT,
            ColumnType.LONG_ARRAY.asTypeString(),
            ColumnType.NESTED_DATA.asTypeString()
        );
      }
      druidType = ColumnType.STRING;
    }
    if (customSchema != null) {
      validateColumnSchemaCustomization(column, customSchema, druidType);
      return customSchema;
    }
    if (druidType.isPrimitive() || druidType.isPrimitiveArray()) {
      // The declared type is retained in the ingestion schema (primitive arrays are cast, rather than left to an
      // untyped auto column whose type is inferred from the ingested values; note that the auto schema stores
      // FLOAT ARRAY as DOUBLE ARRAY).
      return DimensionSchema.getDefaultSchemaForBuiltInType(column.name(), druidType);
    }
    if (druidType.is(ValueType.COMPLEX)) {
      return DimensionHandlerUtils.getComplexDimensionSchema(column.name(), druidType);
    }
    throw InvalidInput.exception(
        "column [%s] has unsupported type [%s] for a clustered base table",
        column.name(),
        druidType
    );
  }

  private void validateColumnSchemaCustomization(
      ColumnSpec column,
      DimensionSchema customSchema,
      ColumnType declaredType
  )
  {
    if (ColumnHolder.TIME_COLUMN_NAME.equals(column.name())) {
      throw InvalidInput.exception(
          "columnSchemas cannot customize [%s]: the time column is always stored as a long",
          ColumnHolder.TIME_COLUMN_NAME
      );
    }
    if (clusteringColumns.contains(column.name())) {
      throw InvalidInput.exception(
          "columnSchemas cannot customize clustering column [%s]: the physical representation of clustering columns"
          + " is fixed by the clustered segment format",
          column.name()
      );
    }
    // The schema's type must match the declared logical type, so the physical schema cannot silently contradict the
    // SQL schema that INSERT/REPLACE queries are validated and coerced against.
    ColumnType expectedType = declaredType;
    if (customSchema instanceof AutoTypeColumnSchema) {
      // An uncast auto column stores values as they are ingested (inferring the physical type) rather than coercing
      // them to the declared type; only a column declared COMPLEX<json> may store arbitrary shapes.
      if (((AutoTypeColumnSchema) customSchema).getCastToType() == null
          && !ColumnType.NESTED_DATA.equals(declaredType)) {
        throw InvalidInput.exception(
            "columnSchemas entry [%s] is an auto column schema without a castToType; an uncast auto column stores"
            + " values as they are ingested rather than coercing them to the column's declared type [%s], set"
            + " castToType to match the declared type",
            column.name(),
            declaredType
        );
      }
      // The auto schema stores FLOAT as DOUBLE.
      expectedType = autoColumnType(declaredType);
    }
    if (!expectedType.equals(customSchema.getColumnType())) {
      throw InvalidInput.exception(
          "columnSchemas entry [%s] of type [%s] does not match the column's declared type [%s]; column schemas"
          + " customize the physical representation of a declared column, not its type",
          column.name(),
          customSchema.getColumnType(),
          declaredType
      );
    }
  }

  /**
   * The type the auto schema stores for a declared type: {@link AutoTypeColumnSchema} coerces FLOAT to DOUBLE (the
   * default derivation for declared FLOAT ARRAY columns relies on the same coercion).
   */
  private static ColumnType autoColumnType(ColumnType declaredType)
  {
    if (ColumnType.FLOAT.equals(declaredType)) {
      return ColumnType.DOUBLE;
    }
    if (ColumnType.FLOAT_ARRAY.equals(declaredType)) {
      return ColumnType.DOUBLE_ARRAY;
    }
    return declaredType;
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClusteredValueGroupsBaseTableMetadata that = (ClusteredValueGroupsBaseTableMetadata) o;
    return Objects.equals(clusteringColumns, that.clusteringColumns)
           && Objects.equals(virtualColumns, that.virtualColumns)
           && Objects.equals(columnSchemas, that.columnSchemas);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(clusteringColumns, virtualColumns, columnSchemas);
  }

  @Override
  public String toString()
  {
    return "ClusteredValueGroupsBaseTableMetadata{" +
           "clusteringColumns=" + clusteringColumns +
           ", virtualColumns=" + virtualColumns +
           ", columnSchemas=" + columnSchemas +
           '}';
  }
}
