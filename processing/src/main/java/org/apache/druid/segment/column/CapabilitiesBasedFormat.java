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

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.DimensionHandler;
import org.apache.druid.segment.DimensionHandlerUtils;

import javax.annotation.Nullable;
import java.util.Objects;

public class CapabilitiesBasedFormat implements ColumnFormat
{
  // merge logic for the state capabilities will be in after incremental index is persisted
  @VisibleForTesting
  public static final ColumnCapabilities.CoercionLogic DIMENSION_CAPABILITY_MERGE_LOGIC =
      new ColumnCapabilities.CoercionLogic()
      {
        @Override
        public boolean dictionaryEncoded()
        {
          return true;
        }

        @Override
        public boolean dictionaryValuesSorted()
        {
          return true;
        }

        @Override
        public boolean dictionaryValuesUnique()
        {
          return true;
        }

        @Override
        public boolean multipleValues()
        {
          return false;
        }

        @Override
        public boolean hasNulls()
        {
          return false;
        }
      };
  private final ColumnCapabilities capabilities;

  public static CapabilitiesBasedFormat forColumnIndexer(ColumnCapabilities capabilities)
  {
    return new CapabilitiesBasedFormat(ColumnCapabilitiesImpl.snapshot(capabilities, DIMENSION_CAPABILITY_MERGE_LOGIC));
  }

  public CapabilitiesBasedFormat(ColumnCapabilities capabilities)
  {
    this.capabilities = capabilities;
  }

  @Override
  public DimensionHandler getColumnHandler(String columnName)
  {
    return DimensionHandlerUtils.getHandlerFromCapabilities(columnName, capabilities, null);
  }

  @Override
  public DimensionSchema getColumnSchema(String columnName)
  {
    return getColumnHandler(columnName).getDimensionSchema(capabilities);
  }

  @Override
  public ColumnFormat merge(@Nullable ColumnFormat otherFormat)
  {
    if (otherFormat == null) {
      return this;
    }

    ColumnCapabilitiesImpl merged = ColumnCapabilitiesImpl.copyOf(this.toColumnCapabilities());
    ColumnCapabilitiesImpl otherSnapshot = ColumnCapabilitiesImpl.copyOf(otherFormat.toColumnCapabilities());

    if (!Objects.equals(merged.getType(), otherSnapshot.getType())
        || !Objects.equals(merged.getElementType(), otherSnapshot.getElementType())) {
      final String mergedType = merged.getType() == null ? null : merged.asTypeString();
      final String otherType = otherSnapshot.getType() == null ? null : otherSnapshot.asTypeString();
      throw new ISE(
          "Cannot merge columns of type[%s] and [%s]",
          mergedType,
          otherType
      );
    } else if (!Objects.equals(merged.getComplexTypeName(), otherSnapshot.getComplexTypeName())) {
      throw new ISE(
          "Cannot merge columns of type[%s] and [%s]",
          merged.getComplexTypeName(),
          otherSnapshot.getComplexTypeName()
      );
    }

    merged.setDictionaryEncoded(merged.isDictionaryEncoded().or(otherSnapshot.isDictionaryEncoded()).isTrue());
    merged.setHasMultipleValues(merged.hasMultipleValues().or(otherSnapshot.hasMultipleValues()).isTrue());
    merged.setDictionaryValuesSorted(
        merged.areDictionaryValuesSorted().and(otherSnapshot.areDictionaryValuesSorted()).isTrue()
    );
    merged.setDictionaryValuesUnique(
        merged.areDictionaryValuesUnique().and(otherSnapshot.areDictionaryValuesUnique()).isTrue()
    );
    merged.setHasNulls(merged.hasNulls().or(otherSnapshot.hasNulls()).isTrue());
    // when merging persisted queryableIndexes in the same ingestion job, all queryableIndexes should have the exact
    // same hasBitmapIndexes flag set which is set in the ingestionSpec.. the exception is explicit null value
    // columns which always report as having bitmap indexes because they can always resolve to all-true or all-false
    // depending on whether or not the filter matches the null value. choosing false here picks what is most likely
    // to be correct since explicit falses mean that the ingestion spec does not have bitmap index creation set
    if (merged.hasBitmapIndexes() != otherSnapshot.hasBitmapIndexes()) {
      merged.setHasBitmapIndexes(false);
    }
    if (merged.hasSpatialIndexes() != otherSnapshot.hasSpatialIndexes()) {
      merged.setHasSpatialIndexes(merged.hasSpatialIndexes() || otherSnapshot.hasSpatialIndexes());
    }

    return new CapabilitiesBasedFormat(merged);
  }

  @Override
  public ColumnType getLogicalType()
  {
    return capabilities.toColumnType();
  }

  @Override
  public ColumnCapabilities toColumnCapabilities()
  {
    return capabilities;
  }
}
