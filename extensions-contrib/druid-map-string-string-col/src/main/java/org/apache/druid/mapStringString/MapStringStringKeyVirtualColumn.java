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

package org.apache.druid.mapStringString;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseSingleValueDimensionSelector;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.virtual.VirtualColumnCacheHelper;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

public class MapStringStringKeyVirtualColumn implements VirtualColumn
{
  private final String columnName;
  private final String key;
  private final String outputName;

  @JsonCreator
  public MapStringStringKeyVirtualColumn(
      @JsonProperty("columnName") String columnName,
      @JsonProperty("key") String key,
      @JsonProperty("outputName") String outputName
  )
  {
    this.columnName = columnName;
    this.key = key;
    this.outputName = outputName;
  }

  @Override
  @JsonProperty
  public String getOutputName()
  {
    return outputName;
  }

  @JsonProperty
  public String getColumnName()
  {
    return columnName;
  }

  @JsonProperty
  public String getKey()
  {
    return key;
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      DimensionSpec dimensionSpec,
      ColumnSelectorFactory factory
  )
  {
    ExtractionFn extractionFn = dimensionSpec.getExtractionFn();

    return new BaseSingleValueDimensionSelector()
    {
      @Nullable
      @Override
      protected String getValue()
      {
        MapStringStringRow rowVal = ((MapStringStringRow) factory.makeColumnValueSelector(columnName).getObject());
        String value = rowVal.getValue(key);
        if (extractionFn != null) {
          value = extractionFn.apply(value);
        }
        return value;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {

      }
    };
  }

  @Override
  public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec, ColumnSelector columnSelector, ReadableOffset offset)
  {
    return dimensionSpec.decorate(
        getMultiStringDimComplexColumn(columnSelector).makeDimensionSelector(key, offset, dimensionSpec.getExtractionFn())
    );
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      String columnName,
      ColumnSelectorFactory factory
  )
  {
    return makeDimensionSelector(new DefaultDimensionSpec(columnName, columnName), factory);
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(String columnName, ColumnSelector columnSelector, ReadableOffset offset)
  {
    return makeDimensionSelector(new DefaultDimensionSpec(columnName, columnName), columnSelector, offset);
  }

  @Override
  public BitmapIndex getBitmapIndex(String columnName, ColumnSelector selector)
  {
    return getMultiStringDimComplexColumn(selector).makeBitmapIndex(key);
  }

  @Override
  public ColumnCapabilities capabilities(String columnName)
  {
    return new ColumnCapabilitiesImpl()
        .setType(ValueType.STRING)
        .setHasMultipleValues(false)
        .setDictionaryEncoded(true)
        .setHasBitmapIndexes(true);
  }

  @Override
  public List<String> requiredColumns()
  {
    return Collections.singletonList(columnName);
  }

  @Override
  public boolean usesDotNotation()
  {
    return false;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(VirtualColumnCacheHelper.CACHE_TYPE_ID_USER_DEFINED)
        .appendByte((byte) 1)
        .appendString(columnName)
        .appendString(key)
        .appendString(outputName)
        .build();
  }

  private MapStringStringComplexColumn getMultiStringDimComplexColumn(ColumnSelector columnSelector)
  {
    return toMultiStringDimComplexColumn(columnSelector.getColumnHolder(columnName).getColumn());
  }

  private MapStringStringComplexColumn toMultiStringDimComplexColumn(BaseColumn column)
  {
    if (column instanceof MapStringStringComplexColumn) {
      return (MapStringStringComplexColumn) column;
    } else {
      throw new IAE("Unrecognized base column type [%s].", column.getClass().getName());
    }
  }
}
