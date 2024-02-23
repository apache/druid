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

package org.apache.druid.segment.nested;

import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.AutoTypeColumnMerger;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.DimensionHandler;
import org.apache.druid.segment.NestedCommonFormatColumnHandler;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnFormat;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.StringUtf8DictionaryEncodedColumn;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.serde.NestedCommonFormatColumnPartSerde;

import javax.annotation.Nullable;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Base implementation for columns created with {@link AutoTypeColumnSchema} and handled with
 * {@link NestedCommonFormatColumnHandler} to allow ease of merge via
 * {@link AutoTypeColumnMerger} by providing a common implementation. All columns are read with
 * {@link NestedCommonFormatColumnPartSerde}
 *
 * @see ScalarDoubleColumn
 * @see ScalarLongColumn
 * @see StringUtf8DictionaryEncodedColumn
 * @see VariantColumn
 * @see CompressedNestedDataComplexColumn
 */
public interface NestedCommonFormatColumn extends BaseColumn
{
  default Indexed<String> getStringDictionary()
  {
    return Indexed.empty();
  }

  default Indexed<Long> getLongDictionary()
  {
    return Indexed.empty();
  }

  default Indexed<Double> getDoubleDictionary()
  {
    return Indexed.empty();
  }

  default Indexed<Object[]> getArrayDictionary()
  {
    return Indexed.empty();
  }


  default SortedMap<String, FieldTypeInfo.MutableTypeSet> getFieldTypeInfo()
  {
    SortedMap<String, FieldTypeInfo.MutableTypeSet> fields = new TreeMap<>();
    if (!ColumnType.NESTED_DATA.equals(getLogicalType())) {
      FieldTypeInfo.MutableTypeSet rootOnlyType = new FieldTypeInfo.MutableTypeSet().add(getLogicalType());
      fields.put(NestedPathFinder.JSON_PATH_ROOT, rootOnlyType);
    }
    return fields;
  }

  ColumnType getLogicalType();

  class Format implements ColumnFormat
  {
    private final ColumnType logicalType;
    private final boolean hasNulls;
    private final boolean enforceLogicalType;

    public Format(ColumnType logicalType, boolean hasNulls, boolean enforceLogicalType)
    {
      this.logicalType = logicalType;
      this.hasNulls = hasNulls;
      this.enforceLogicalType = enforceLogicalType;
    }

    @Override
    public ColumnType getLogicalType()
    {
      return logicalType;
    }

    @Override
    public DimensionHandler getColumnHandler(String columnName)
    {
      return new NestedCommonFormatColumnHandler(columnName, enforceLogicalType ? logicalType : null);
    }

    @Override
    public DimensionSchema getColumnSchema(String columnName)
    {
      return new AutoTypeColumnSchema(columnName, enforceLogicalType ? logicalType : null);
    }

    @Override
    public ColumnFormat merge(@Nullable ColumnFormat otherFormat)
    {
      if (otherFormat == null) {
        return this;
      }

      if (otherFormat instanceof Format) {
        final Format other = (Format) otherFormat;
        if (!getLogicalType().equals(other.getLogicalType())) {
          return new Format(ColumnType.NESTED_DATA, hasNulls || other.hasNulls, false);
        }
        return new Format(logicalType, hasNulls || other.hasNulls, enforceLogicalType || other.enforceLogicalType);
      }
      throw new ISE(
          "Cannot merge columns of type[%s] and format[%s] and with [%s] and [%s]",
          logicalType,
          this.getClass().getName(),
          otherFormat.getLogicalType(),
          otherFormat.getClass().getName()
      );
    }

    @Override
    public ColumnCapabilities toColumnCapabilities()
    {
      if (logicalType.isPrimitive() || logicalType.isArray()) {
        return ColumnCapabilitiesImpl.createDefault()
                                     .setType(logicalType)
                                     .setDictionaryEncoded(true)
                                     .setDictionaryValuesSorted(true)
                                     .setDictionaryValuesUnique(true)
                                     .setHasBitmapIndexes(true)
                                     .setHasNulls(hasNulls);
      }
      return ColumnCapabilitiesImpl.createDefault().setType(logicalType).setHasNulls(hasNulls);
    }
  }
}
