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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.DimensionHandler;
import org.apache.druid.segment.StandardTypeColumnHandler;
import org.apache.druid.segment.StandardTypeColumnSchema;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.nested.FieldTypeInfo;
import org.apache.druid.segment.nested.NestedPathFinder;

import javax.annotation.Nullable;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Base implementation for columns created with {@link StandardTypeColumnSchema} and handled with
 * {@link StandardTypeColumnHandler} to allow ease of merge via
 * {@link org.apache.druid.segment.StandardTypeColumnMerger}
 */
public interface StandardTypeColumn extends BaseColumn
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

  default void mergeNestedFields(SortedMap<String, FieldTypeInfo.MutableTypeSet> mergedFields)
  {
    FieldTypeInfo.MutableTypeSet rootOnlyType = new FieldTypeInfo.MutableTypeSet().add(getLogicalType());
    mergedFields.compute(NestedPathFinder.JSON_PATH_ROOT, (k, v) -> {
      if (v == null) {
        return rootOnlyType;
      }
      return v.merge(rootOnlyType.getByteValue());
    });
  }


  default SortedMap<String, FieldTypeInfo.MutableTypeSet> getFieldTypeInfo()
  {
    FieldTypeInfo.MutableTypeSet rootOnlyType = new FieldTypeInfo.MutableTypeSet().add(getLogicalType());
    SortedMap<String, FieldTypeInfo.MutableTypeSet> fields = new TreeMap<>();
    fields.put(NestedPathFinder.JSON_PATH_ROOT, rootOnlyType);
    return fields;
  }

  ColumnType getLogicalType();

  class Format implements ColumnFormat
  {
    private final ColumnType logicalType;
    private final boolean hasNulls;

    public Format(ColumnType logicalType, boolean hasNulls)
    {
      this.logicalType = logicalType;
      this.hasNulls = hasNulls;
    }

    @Override
    public ColumnType getLogicalType()
    {
      return logicalType;
    }

    @Override
    public DimensionHandler getColumnHandler(String columnName)
    {
      return new StandardTypeColumnHandler(columnName);
    }

    @Override
    public DimensionSchema getColumnSchema(String columnName)
    {
      return new StandardTypeColumnSchema(columnName);
    }

    @Override
    public ColumnFormat merge(@Nullable ColumnFormat otherFormat)
    {
      if (otherFormat == null) {
        return this;
      }

      if (otherFormat instanceof Format) {
        final boolean otherHasNulls = ((Format) otherFormat).hasNulls;
        if (!getLogicalType().equals(otherFormat.getLogicalType())) {
          return new Format(ColumnType.NESTED_DATA, hasNulls || otherHasNulls);
        }
        return new Format(logicalType, hasNulls || otherHasNulls);
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
