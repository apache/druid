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

  public static DimensionSchema createDimensionSchema(
      final String column,
      @Nullable final ColumnType type,
      boolean useAutoType,
      ArrayIngestMode arrayIngestMode
  )
  {
    if (useAutoType) {
      // for complex types that are not COMPLEX<json>, we still want to use the handler since 'auto' typing
      // only works for the 'standard' built-in types
      if (type != null && type.is(ValueType.COMPLEX) && !ColumnType.NESTED_DATA.equals(type)) {
        final ColumnCapabilities capabilities = ColumnCapabilitiesImpl.createDefault().setType(type);
        return DimensionHandlerUtils.getHandlerFromCapabilities(column, capabilities, null)
                                    .getDimensionSchema(capabilities);
      }

      if (type != null && (type.isPrimitive() || type.isPrimitiveArray())) {
        return new AutoTypeColumnSchema(column, type);
      }
      return new AutoTypeColumnSchema(column, null);
    } else {
      // if schema information is not available, create a string dimension
      if (type == null) {
        return new StringDimensionSchema(column);
      } else if (type.getType() == ValueType.STRING) {
        return new StringDimensionSchema(column);
      } else if (type.getType() == ValueType.LONG) {
        return new LongDimensionSchema(column);
      } else if (type.getType() == ValueType.FLOAT) {
        return new FloatDimensionSchema(column);
      } else if (type.getType() == ValueType.DOUBLE) {
        return new DoubleDimensionSchema(column);
      } else if (type.getType() == ValueType.ARRAY) {
        ValueType elementType = type.getElementType().getType();
        if (elementType == ValueType.STRING) {
          if (arrayIngestMode == ArrayIngestMode.NONE) {
            throw InvalidInput.exception(
                "String arrays can not be ingested when '%s' is set to '%s'. Set '%s' in query context "
                + "to 'array' to ingest the string array as an array, or ingest it as an MVD by explicitly casting the "
                + "array to an MVD with ARRAY_TO_MV function.",
                MultiStageQueryContext.CTX_ARRAY_INGEST_MODE,
                StringUtils.toLowerCase(arrayIngestMode.name()),
                MultiStageQueryContext.CTX_ARRAY_INGEST_MODE
            );
          } else if (arrayIngestMode == ArrayIngestMode.MVD) {
            return new StringDimensionSchema(column, DimensionSchema.MultiValueHandling.ARRAY, null);
          } else {
            // arrayIngestMode == ArrayIngestMode.ARRAY would be true
            return new AutoTypeColumnSchema(column, type);
          }
        } else if (elementType.isNumeric()) {
          // ValueType == LONG || ValueType == FLOAT || ValueType == DOUBLE
          if (arrayIngestMode == ArrayIngestMode.ARRAY) {
            return new AutoTypeColumnSchema(column, type);
          } else {
            throw InvalidInput.exception(
                "Numeric arrays can only be ingested when '%s' is set to 'array' in the MSQ query's context. "
                + "Current value of the parameter [%s]",
                MultiStageQueryContext.CTX_ARRAY_INGEST_MODE,
                StringUtils.toLowerCase(arrayIngestMode.name())
            );
          }
        } else {
          throw new ISE("Cannot create dimension for type [%s]", type.toString());
        }
      } else {
        final ColumnCapabilities capabilities = ColumnCapabilitiesImpl.createDefault().setType(type);
        return DimensionHandlerUtils.getHandlerFromCapabilities(column, capabilities, null)
                                    .getDimensionSchema(capabilities);
      }
    }
  }
}
