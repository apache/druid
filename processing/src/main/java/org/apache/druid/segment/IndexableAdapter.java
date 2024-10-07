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

package org.apache.druid.segment;

import com.google.errorprone.annotations.MustBeClosed;
import org.apache.druid.segment.column.CapabilitiesBasedFormat;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnFormat;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.data.BitmapValues;
import org.apache.druid.segment.data.CloseableIndexed;
import org.apache.druid.segment.nested.FieldTypeInfo;
import org.apache.druid.segment.nested.SortedValueDictionary;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

/**
 * An adapter to an index
 */
public interface IndexableAdapter
{
  Interval getDataInterval();

  int getNumRows();

  /**
   * Returns names of dimension columns.
   *
   * @param includeTime whether to include {@link ColumnHolder#TIME_COLUMN_NAME}.
   */
  List<String> getDimensionNames(boolean includeTime);

  List<String> getMetricNames();

  @MustBeClosed
  @Nullable
  <T extends Comparable<? super T>> CloseableIndexed<T> getDimValueLookup(String dimension);

  @Nullable
  NestedColumnMergable getNestedColumnMergeables(String column);

  TransformableRowIterator getRows();

  IndexableAdapter getProjectionAdapter(String projection);

  BitmapValues getBitmapValues(String dimension, int dictId);

  ColumnCapabilities getCapabilities(String column);

  default ColumnFormat getFormat(String column)
  {
    return new CapabilitiesBasedFormat(getCapabilities(column));
  }

  Metadata getMetadata();

  class NestedColumnMergable implements Closeable
  {
    private final SortedValueDictionary valueDictionary;
    private final SortedMap<String, FieldTypeInfo.MutableTypeSet> fields;
    private final boolean forceNestedType;
    private final boolean isConstant;
    @Nullable
    private final Object constantValue;

    public NestedColumnMergable(
        SortedValueDictionary valueDictionary,
        SortedMap<String, FieldTypeInfo.MutableTypeSet> fields,
        boolean forceNestedType,
        boolean isConstant,
        @Nullable Object constantValue
    )
    {
      this.valueDictionary = valueDictionary;
      this.fields = fields;
      this.forceNestedType = forceNestedType;
      this.isConstant = isConstant;
      this.constantValue = constantValue;
    }

    public boolean isForceNestedType()
    {
      return forceNestedType;
    }

    public boolean isConstant()
    {
      return isConstant;
    }

    @Nullable
    public Object getConstantValue()
    {
      return constantValue;
    }

    @Nullable
    public SortedValueDictionary getValueDictionary()
    {
      return valueDictionary;
    }

    public void mergeFieldsInto(SortedMap<String, FieldTypeInfo.MutableTypeSet> mergeInto)
    {
      for (Map.Entry<String, FieldTypeInfo.MutableTypeSet> entry : fields.entrySet()) {
        final String fieldPath = entry.getKey();
        final FieldTypeInfo.MutableTypeSet types = entry.getValue();
        mergeInto.compute(fieldPath, (k, v) -> {
          if (v == null) {
            return types;
          }
          return v.merge(types.getByteValue(), types.hasUntypedArray());
        });
      }
    }

    @Override
    public void close() throws IOException
    {
      valueDictionary.close();
    }
  }
}
