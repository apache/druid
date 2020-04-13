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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.DimFilterUtils;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.virtual.VirtualColumnCacheHelper;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class MapVirtualColumn implements VirtualColumn
{
  private final String outputName;
  private final String keyDimension;
  private final String valueDimension;

  @JsonCreator
  public MapVirtualColumn(
      @JsonProperty("keyDimension") String keyDimension,
      @JsonProperty("valueDimension") String valueDimension,
      @JsonProperty("outputName") String outputName
  )
  {
    Preconditions.checkArgument(keyDimension != null, "key dimension should not be null");
    Preconditions.checkArgument(valueDimension != null, "value dimension should not be null");
    Preconditions.checkArgument(outputName != null, "output name should not be null");

    this.keyDimension = keyDimension;
    this.valueDimension = valueDimension;
    this.outputName = outputName;
  }

  @Override
  public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec, ColumnSelectorFactory factory)
  {
    final DimensionSelector keySelector = factory.makeDimensionSelector(DefaultDimensionSpec.of(keyDimension));
    final DimensionSelector valueSelector = factory.makeDimensionSelector(DefaultDimensionSpec.of(valueDimension));
    final String subColumnName = VirtualColumns.splitColumnName(dimensionSpec.getDimension()).rhs;
    if (subColumnName == null) {
      return dimensionSpec.decorate(new MapTypeMapVirtualColumnDimensionSelector(keySelector, valueSelector));
    } else {
      return dimensionSpec.decorate(
          new StringTypeMapVirtualColumnDimensionSelector(keySelector, valueSelector, subColumnName)
      );
    }
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(String columnName, ColumnSelectorFactory factory)
  {
    final DimensionSelector keySelector = factory.makeDimensionSelector(DefaultDimensionSpec.of(keyDimension));
    final DimensionSelector valueSelector = factory.makeDimensionSelector(DefaultDimensionSpec.of(valueDimension));

    final String subColumnName = VirtualColumns.splitColumnName(columnName).rhs;

    if (subColumnName == null) {
      return new MapVirtualColumnValueSelector<Map>(keySelector, valueSelector)
      {
        @Override
        public Class<Map> classOfObject()
        {
          return Map.class;
        }

        @Override
        public Map getObject()
        {
          final IndexedInts keyIndices = keySelector.getRow();
          final IndexedInts valueIndices = valueSelector.getRow();
          final int limit = Math.min(keyIndices.size(), valueIndices.size());
          final Map<String, String> map = Maps.newHashMapWithExpectedSize(limit);
          for (int i = 0; i < limit; i++) {
            map.put(
                keySelector.lookupName(keyIndices.get(i)),
                valueSelector.lookupName(valueIndices.get(i))
            );
          }
          return map;
        }
      };
    }

    IdLookup keyIdLookup = keySelector.idLookup();
    if (keyIdLookup != null) {
      final int keyId = keyIdLookup.lookupId(subColumnName);
      if (keyId < 0) {
        return NilColumnValueSelector.instance();
      }
      return new MapVirtualColumnValueSelector<String>(keySelector, valueSelector)
      {
        @Override
        public Class<String> classOfObject()
        {
          return String.class;
        }

        @Nullable
        @Override
        public String getObject()
        {
          final IndexedInts keyIndices = keySelector.getRow();
          final IndexedInts valueIndices = valueSelector.getRow();
          final int limit = Math.min(keyIndices.size(), valueIndices.size());
          for (int i = 0; i < limit; i++) {
            if (keyIndices.get(i) == keyId) {
              return valueSelector.lookupName(valueIndices.get(i));
            }
          }
          return null;
        }
      };
    } else {
      return new MapVirtualColumnValueSelector<String>(keySelector, valueSelector)
      {
        @Override
        public Class<String> classOfObject()
        {
          return String.class;
        }

        @Nullable
        @Override
        public String getObject()
        {
          final IndexedInts keyIndices = keySelector.getRow();
          final IndexedInts valueIndices = valueSelector.getRow();
          final int limit = Math.min(keyIndices.size(), valueIndices.size());
          for (int i = 0; i < limit; i++) {
            if (Objects.equals(keySelector.lookupName(keyIndices.get(i)), subColumnName)) {
              return valueSelector.lookupName(valueIndices.get(i));
            }
          }
          return null;
        }
      };
    }
  }

  @Override
  public ColumnCapabilities capabilities(String columnName)
  {
    final ValueType valueType = columnName.indexOf('.') < 0 ? ValueType.COMPLEX : ValueType.STRING;
    return new ColumnCapabilitiesImpl().setType(valueType);
  }

  @Override
  public List<String> requiredColumns()
  {
    return ImmutableList.of(keyDimension, valueDimension);
  }

  @Override
  public boolean usesDotNotation()
  {
    return true;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] key = StringUtils.toUtf8(keyDimension);
    byte[] value = StringUtils.toUtf8(valueDimension);
    byte[] output = StringUtils.toUtf8(outputName);

    return ByteBuffer.allocate(3 + key.length + value.length + output.length)
                     .put(VirtualColumnCacheHelper.CACHE_TYPE_ID_MAP)
                     .put(key).put(DimFilterUtils.STRING_SEPARATOR)
                     .put(value).put(DimFilterUtils.STRING_SEPARATOR)
                     .put(output)
                     .array();
  }

  @JsonProperty
  public String getKeyDimension()
  {
    return keyDimension;
  }

  @JsonProperty
  public String getValueDimension()
  {
    return valueDimension;
  }

  @Override
  @JsonProperty
  public String getOutputName()
  {
    return outputName;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MapVirtualColumn)) {
      return false;
    }

    MapVirtualColumn that = (MapVirtualColumn) o;

    if (!keyDimension.equals(that.keyDimension)) {
      return false;
    }
    if (!valueDimension.equals(that.valueDimension)) {
      return false;
    }
    if (!outputName.equals(that.outputName)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = keyDimension.hashCode();
    result = 31 * result + valueDimension.hashCode();
    result = 31 * result + outputName.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "MapVirtualColumn{" +
           "keyDimension='" + keyDimension + '\'' +
           ", valueDimension='" + valueDimension + '\'' +
           ", outputName='" + outputName + '\'' +
           '}';
  }
}
