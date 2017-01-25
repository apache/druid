/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.metamx.common.StringUtils;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.DimFilterUtils;
import io.druid.segment.data.IndexedInts;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

/**
 */
public class MapVirtualColumn implements VirtualColumn
{
  private static final byte VC_TYPE_ID = 0x00;

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
  public ObjectColumnSelector init(String dimension, ColumnSelectorFactory factory)
  {
    final DimensionSelector keySelector = factory.makeDimensionSelector(DefaultDimensionSpec.of(keyDimension));
    final DimensionSelector valueSelector = factory.makeDimensionSelector(DefaultDimensionSpec.of(valueDimension));

    int index = dimension.indexOf('.');
    if (index < 0) {
      return new ObjectColumnSelector<Map>()
      {
        @Override
        public Class classOfObject()
        {
          return Map.class;
        }

        @Override
        public Map get()
        {
          final IndexedInts keyIndices = keySelector.getRow();
          final IndexedInts valueIndices = valueSelector.getRow();
          if (keyIndices == null || valueIndices == null) {
            return null;
          }
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
      final int keyId = keyIdLookup.lookupId(dimension.substring(index + 1));
      if (keyId < 0) {
        return NullStringObjectColumnSelector.instance();
      }
      return new ObjectColumnSelector<String>()
      {
        @Override
        public Class classOfObject()
        {
          return String.class;
        }

        @Override
        public String get()
        {
          final IndexedInts keyIndices = keySelector.getRow();
          final IndexedInts valueIndices = valueSelector.getRow();
          if (keyIndices == null || valueIndices == null) {
            return null;
          }
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
      final String key = dimension.substring(index + 1);
      return new ObjectColumnSelector<String>()
      {
        @Override
        public Class classOfObject()
        {
          return String.class;
        }

        @Override
        public String get()
        {
          final IndexedInts keyIndices = keySelector.getRow();
          final IndexedInts valueIndices = valueSelector.getRow();
          if (keyIndices == null || valueIndices == null) {
            return null;
          }
          final int limit = Math.min(keyIndices.size(), valueIndices.size());
          for (int i = 0; i < limit; i++) {
            if (Objects.equals(keySelector.lookupName(keyIndices.get(i)), key)) {
              return valueSelector.lookupName(valueIndices.get(i));
            }
          }
          return null;
        }
      };
    }
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
                     .put(VC_TYPE_ID)
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
