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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;

/**
 * Dimension-schema-related utility functions that would make sense in {@link DimensionSchema} if this
 * were not an extension.
 */
public class DimensionSchemaUtils
{
  public static DimensionSchema createDimensionSchema(final String column, @Nullable final ColumnType type)
  {
    // if schema information not available, create a string dimension
    if (type == null) {
      return new StringDimensionSchema(column);
    }

    switch (type.getType()) {
      case STRING:
        return new StringDimensionSchema(column);
      case LONG:
        return new LongDimensionSchema(column);
      case FLOAT:
        return new FloatDimensionSchema(column);
      case DOUBLE:
        return new DoubleDimensionSchema(column);
      case ARRAY:
        switch (type.getElementType().getType()) {
          case STRING:
            return new StringDimensionSchema(column, DimensionSchema.MultiValueHandling.ARRAY, null);
          default:
            throw new ISE("Cannot create dimension for type [%s]", type.toString());
        }
      default:
        final ColumnCapabilities capabilities = ColumnCapabilitiesImpl.createDefault().setType(type);
        return DimensionHandlerUtils.getHandlerFromCapabilities(column, capabilities, null)
                                    .getDimensionSchema(capabilities);
    }
  }
}
