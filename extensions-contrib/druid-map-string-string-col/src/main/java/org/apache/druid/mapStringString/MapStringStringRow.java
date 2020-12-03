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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import org.apache.druid.common.config.NullHandling;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;

public class MapStringStringRow implements Comparable<MapStringStringRow>
{
  private static final Interner<String> STRING_INTERNER = Interners.newWeakInterner();

  public static final Comparator<MapStringStringRow> COMPARATOR = new Comparator<MapStringStringRow>()
  {
    @Override
    public int compare(MapStringStringRow o1, MapStringStringRow o2)
    {
      if (o1 == null) {
        if (o2 == null) {
          return 0;
        } else {
          return -1;
        }
      } else if (o2 == null) {
        return 1;
      } else {
        return o1.compareTo(o2);
      }
    }
  };

  public static final MapStringStringRow EMPTY_INSTANCE = new MapStringStringRow(ImmutableMap.of());

  private final ImmutableMap<String, String> values;

  private MapStringStringRow(ImmutableMap<String, String> values)
  {
    this.values = values;
  }

  public static MapStringStringRow create(Map<String, String> in)
  {
    if (in == null) {
      return EMPTY_INSTANCE;
    }

    Builder builder = new Builder();
    for (Map.Entry<String, String> e : in.entrySet()) {
      builder.put(e.getKey(), e.getValue());
    }

    return builder.build();
  }

  public String getValue(String key)
  {
    return values.get(key);
  }

  public ImmutableMap<String, String> getValues()
  {
    return values;
  }

  public long getEstimatedOnHeapSize()
  {
    // This is a rough approximation, also strings are interned so counting their size directly
    // would severly overestimate the result. This can be further refined later as usage of this column
    // becomes more widespread.
    return values.size() * 2 * Long.BYTES * 4;
  }

  @Override
  public int compareTo(MapStringStringRow o)
  {
    if (values.equals(o.values)) {
      return 0;
    }

    // Note: We just want it to be deterministic
    return values.hashCode() <= o.values.hashCode() ? -1 : 1;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MapStringStringRow that = (MapStringStringRow) o;
    return Objects.equals(values, that.values);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(values);
  }

  @Override
  public String toString()
  {
    return "MapStringStringRow{" +
           "values=" + values +
           '}';
  }

  public static class Builder
  {
    private final ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    public void put(String key, String value)
    {
      value = NullHandling.emptyToNullIfNeeded(value);
      if (key != null && value != null) {
        builder.put(STRING_INTERNER.intern(key), STRING_INTERNER.intern(value));
      }
    }

    public MapStringStringRow build()
    {
      ImmutableMap<String, String> values = builder.build();

      if (values.isEmpty()) {
        return EMPTY_INSTANCE;
      } else {
        return new MapStringStringRow(values);
      }
    }
  }

  public static class Serializer extends JsonSerializer<MapStringStringRow>
  {
    @Override
    public void serialize(MapStringStringRow row, JsonGenerator jgen, SerializerProvider provider)
        throws IOException
    {
      jgen.writeObject(row.getValues());
    }
  }
}
