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

package org.apache.druid.msq.util;

import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;

/**
 * Dimension-schema-related utility functions that would make sense in {@link DimensionSchema} if this
 * were not an extension.
 */
public class DimensionSchemaUtils
{

  /**
   * Creates a dimension schema for creating {@link org.apache.druid.data.input.InputSourceReader}.
   */
  public static DimensionSchema createDimensionSchemaForExtern(final String column, @Nullable final ColumnType type)
  {
    return createDimensionSchema(
        column,
        type,
        false,
        // Least restrictive mode since we do not have any type restrictions while reading the extern files.
        ArrayIngestMode.ARRAY
    );
  }

  /**
   * Create a dimension schema for a dimension column, given the type that it was assigned in the query, and the
   * current values of {@link MultiStageQueryContext#CTX_USE_AUTO_SCHEMAS} and
   * {@link MultiStageQueryContext#CTX_ARRAY_INGEST_MODE}.
   *
   * @param column          column name
   * @param queryType       type of the column from the query
   * @param useAutoType     active value of {@link MultiStageQueryContext#CTX_USE_AUTO_SCHEMAS}
   * @param arrayIngestMode active value of {@link MultiStageQueryContext#CTX_ARRAY_INGEST_MODE}
   */
  public static DimensionSchema createDimensionSchema(
      final String column,
      @Nullable final ColumnType queryType,
      boolean useAutoType,
      ArrayIngestMode arrayIngestMode
  )
  {
    if (useAutoType) {
      // for complex types that are not COMPLEX<json>, we still want to use the handler since 'auto' typing
      // only works for the 'standard' built-in types
      if (queryType != null && queryType.is(ValueType.COMPLEX) && !ColumnType.NESTED_DATA.equals(queryType)) {
        final ColumnCapabilities capabilities = ColumnCapabilitiesImpl.createDefault().setType(queryType);
        return DimensionHandlerUtils.getHandlerFromCapabilities(column, capabilities, null)
                                    .getDimensionSchema(capabilities);
      }

      if (queryType != null && (queryType.isPrimitive() || queryType.isPrimitiveArray())) {
        return new AutoTypeColumnSchema(column, queryType);
      }
      return new AutoTypeColumnSchema(column, null);
    } else {
      // dimensionType may not be identical to queryType, depending on arrayIngestMode.
      final ColumnType dimensionType = getDimensionType(queryType, arrayIngestMode);

      if (dimensionType.getType() == ValueType.STRING) {
        return new StringDimensionSchema(
            column,
            queryType != null && queryType.isArray()
            ? DimensionSchema.MultiValueHandling.ARRAY
            : DimensionSchema.MultiValueHandling.SORTED_ARRAY,
            null
        );
      } else if (dimensionType.getType() == ValueType.LONG) {
        return new LongDimensionSchema(column);
      } else if (dimensionType.getType() == ValueType.FLOAT) {
        return new FloatDimensionSchema(column);
      } else if (dimensionType.getType() == ValueType.DOUBLE) {
        return new DoubleDimensionSchema(column);
      } else if (dimensionType.getType() == ValueType.ARRAY) {
        return new AutoTypeColumnSchema(column, dimensionType);
      } else {
        final ColumnCapabilities capabilities = ColumnCapabilitiesImpl.createDefault().setType(dimensionType);
        return DimensionHandlerUtils.getHandlerFromCapabilities(column, capabilities, null)
                                    .getDimensionSchema(capabilities);
      }
    }
  }

  /**
   * Based on a type from a query result, get the type of dimension we should write.
   *
   * @throws org.apache.druid.error.DruidException if there is some problem
   */
  public static ColumnType getDimensionType(
      @Nullable final ColumnType queryType,
      final ArrayIngestMode arrayIngestMode
  )
  {
    if (queryType == null) {
      // if schema information is not available, create a string dimension
      return ColumnType.STRING;
    } else if (queryType.getType() == ValueType.ARRAY) {
      ValueType elementType = queryType.getElementType().getType();
      if (elementType == ValueType.STRING) {
        if (arrayIngestMode == ArrayIngestMode.NONE) {
          throw InvalidInput.exception(
              "String arrays can not be ingested when '%s' is set to '%s'. Set '%s' in query context "
              + "to 'array' to ingest the string array as an array, or ingest it as an MVD by explicitly casting the "
              + "array to an MVD with the ARRAY_TO_MV function.",
              MultiStageQueryContext.CTX_ARRAY_INGEST_MODE,
              StringUtils.toLowerCase(arrayIngestMode.name()),
              MultiStageQueryContext.CTX_ARRAY_INGEST_MODE
          );
        } else if (arrayIngestMode == ArrayIngestMode.MVD) {
          return ColumnType.STRING;
        } else {
          assert arrayIngestMode == ArrayIngestMode.ARRAY;
          return queryType;
        }
      } else if (elementType.isNumeric()) {
        // ValueType == LONG || ValueType == FLOAT || ValueType == DOUBLE
        if (arrayIngestMode == ArrayIngestMode.ARRAY) {
          return queryType;
        } else {
          throw InvalidInput.exception(
              "Numeric arrays can only be ingested when '%s' is set to 'array'. "
              + "Current value of the parameter is[%s]",
              MultiStageQueryContext.CTX_ARRAY_INGEST_MODE,
              StringUtils.toLowerCase(arrayIngestMode.name())
          );
        }
      } else {
        throw new ISE("Cannot create dimension for type[%s]", queryType.toString());
      }
    } else {
      return queryType;
    }
  }
}
