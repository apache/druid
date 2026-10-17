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

import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared logic for {@link DatasourceBaseTableMetadata} implementations to derive the physical column list of a
 * base-table spec from the declared catalog columns, with optional per-column {@link DimensionSchema}
 * customizations. Layout-specific rules (such as which columns a layout forbids customizing) stay with the
 * implementations.
 */
final class BaseTableColumns
{
  /**
   * Indexes per-column schema customizations by column name, rejecting nulls and duplicates.
   */
  static Map<String, DimensionSchema> indexColumnSchemas(List<DimensionSchema> columnSchemas)
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

  /**
   * Derives the {@link DimensionSchema} used during segment creation for one declared catalog column: the
   * customization when one is supplied (validated against the declared type), else the default schema for the
   * declared type.
   */
  static DimensionSchema toDimensionSchema(ColumnSpec column, @Nullable DimensionSchema customSchema, String typeName)
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
        "column [%s] has unsupported type [%s] for a [%s] base table",
        column.name(),
        druidType,
        typeName
    );
  }

  private static void validateColumnSchemaCustomization(
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

  private BaseTableColumns()
  {
  }
}
