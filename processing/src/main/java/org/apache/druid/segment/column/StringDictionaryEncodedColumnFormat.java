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

package org.apache.druid.segment.column;

import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionSchema.MultiValueHandling;
import org.apache.druid.data.input.impl.NewSpatialDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.DimensionHandler;
import org.apache.druid.segment.StringColumnFormatSpec;
import org.apache.druid.segment.StringDimensionHandler;

import javax.annotation.Nullable;
import java.util.Collections;

public class StringDictionaryEncodedColumnFormat implements ColumnFormat
{
  private final boolean hasMultipleValues;
  private final boolean hasNulls;
  private final boolean hasBitmapIndexes;
  private final boolean hasSpatialIndexes;
  @Nullable
  private final StringColumnFormatSpec columnFormatSpec;

  public StringDictionaryEncodedColumnFormat(
      boolean hasMultipleValues,
      boolean hasNulls,
      boolean hasBitmapIndexes,
      boolean hasSpatialIndexes,
      @Nullable StringColumnFormatSpec columnFormatSpec
  )
  {
    this.hasMultipleValues = hasMultipleValues;
    this.hasNulls = hasNulls;
    this.hasBitmapIndexes = hasBitmapIndexes;
    this.hasSpatialIndexes = hasSpatialIndexes;
    this.columnFormatSpec = columnFormatSpec;
  }

  @Override
  public ColumnType getLogicalType()
  {
    return ColumnType.STRING;
  }

  @Override
  public ColumnCapabilities toColumnCapabilities()
  {
    return ColumnCapabilitiesImpl.createDefault()
                                 .setType(ColumnType.STRING)
                                 .setDictionaryEncoded(true)
                                 .setDictionaryValuesSorted(true)
                                 .setDictionaryValuesUnique(true)
                                 .setHasMultipleValues(hasMultipleValues)
                                 .setHasNulls(hasNulls)
                                 .setHasBitmapIndexes(hasBitmapIndexes)
                                 .setHasSpatialIndexes(hasSpatialIndexes);
  }

  @Override
  public DimensionHandler getColumnHandler(String columnName)
  {
    Integer maxStringLength = columnFormatSpec != null ? columnFormatSpec.getMaxStringLength() : null;
    return new StringDimensionHandler(
        columnName,
        MultiValueHandling.ofDefault(),
        hasBitmapIndexes,
        hasSpatialIndexes,
        maxStringLength,
        columnFormatSpec
    );
  }

  @Override
  public DimensionSchema getColumnSchema(String columnName)
  {
    if (hasSpatialIndexes) {
      return new NewSpatialDimensionSchema(columnName, Collections.singletonList(columnName));
    }
    return new StringDimensionSchema(columnName, null, hasBitmapIndexes, columnFormatSpec);
  }

  @Override
  public ColumnFormat merge(@Nullable ColumnFormat otherFormat)
  {
    if (otherFormat == null) {
      return this;
    }

    if (otherFormat instanceof StringDictionaryEncodedColumnFormat) {
      final StringDictionaryEncodedColumnFormat other = (StringDictionaryEncodedColumnFormat) otherFormat;
      return new StringDictionaryEncodedColumnFormat(
          hasMultipleValues || other.hasMultipleValues,
          hasNulls || other.hasNulls,
          hasBitmapIndexes && other.hasBitmapIndexes,
          hasSpatialIndexes || other.hasSpatialIndexes,
          columnFormatSpec != null ? columnFormatSpec : other.columnFormatSpec
      );
    }

    if (otherFormat instanceof CapabilitiesBasedFormat) {
      final ColumnCapabilities otherCaps = otherFormat.toColumnCapabilities();
      return new StringDictionaryEncodedColumnFormat(
          hasMultipleValues || otherCaps.hasMultipleValues().isMaybeTrue(),
          hasNulls || otherCaps.hasNulls().isMaybeTrue(),
          hasBitmapIndexes && otherCaps.hasBitmapIndexes(),
          hasSpatialIndexes || otherCaps.hasSpatialIndexes(),
          columnFormatSpec
      );
    }

    throw new ISE(
        "Cannot merge columns of type[%s] and format[%s] and with [%s] and [%s]",
        ColumnType.STRING,
        this.getClass().getName(),
        otherFormat.getLogicalType(),
        otherFormat.getClass().getName()
    );
  }
}
