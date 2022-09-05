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

package org.apache.druid.data.input.avro;

import com.google.common.collect.ImmutableMap;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.spi.json.JsonProvider;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import javax.annotation.Nullable;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * JsonProvider for JsonPath + Avro.
 */
public class GenericAvroJsonProvider implements JsonProvider
{
  private final boolean extractUnionsByType;

  GenericAvroJsonProvider(final boolean extractUnionsByType)
  {
    this.extractUnionsByType = extractUnionsByType;
  }

  @Override
  public Object parse(final String s) throws InvalidJsonException
  {
    throw new UnsupportedOperationException("Unused");
  }

  @Override
  public Object parse(final InputStream inputStream, final String s) throws InvalidJsonException
  {
    throw new UnsupportedOperationException("Unused");
  }

  @Override
  public String toJson(final Object o)
  {
    throw new UnsupportedOperationException("Unused");
  }

  @Override
  public Object createArray()
  {
    return new ArrayList<>();
  }

  @Override
  public Object createMap()
  {
    return new HashMap<>();
  }

  @Override
  public boolean isArray(final Object o)
  {
    return o instanceof List;
  }

  @Override
  public int length(final Object o)
  {
    if (o instanceof List) {
      return ((List) o).size();
    } else if (o instanceof GenericRecord) {
      return ((GenericRecord) o).getSchema().getFields().size();
    } else {
      return 0;
    }
  }

  @Override
  public Iterable<?> toIterable(final Object o)
  {
    if (o instanceof List) {
      return (List) o;
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public Collection<String> getPropertyKeys(final Object o)
  {
    if (o == null) {
      return Collections.emptySet();
    } else if (o instanceof Map) {
      return ((Map<Object, Object>) o).keySet().stream().map(String::valueOf).collect(Collectors.toSet());
    } else if (o instanceof GenericRecord) {
      return ((GenericRecord) o).getSchema().getFields().stream().map(Schema.Field::name).collect(Collectors.toSet());
    } else {
      throw new UnsupportedOperationException("Unused");
    }
  }

  @Override
  public Object getArrayIndex(final Object o, final int i)
  {
    return ((List) o).get(i);
  }

  @Override
  @Deprecated
  public Object getArrayIndex(final Object o, final int i, final boolean b)
  {
    throw new UnsupportedOperationException("Deprecated");
  }

  @Override
  public void setArrayIndex(final Object o, final int i, final Object o1)
  {
    if (o instanceof List) {
      final List list = (List) o;
      if (list.size() == i) {
        list.add(o1);
      } else {
        list.set(i, o1);
      }
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Nullable
  @Override
  public Object getMapValue(final Object o, final String s)
  {
    if (o == null) {
      return null;
    } else if (o instanceof GenericRecord) {
      final GenericRecord record = (GenericRecord) o;
      if (extractUnionsByType && isExtractableUnion(record.getSchema().getField(s))) {
        return extractUnionTypes(record.get(s));
      }
      return record.get(s);
    } else if (o instanceof Map) {
      final Map theMap = (Map) o;
      if (theMap.containsKey(s)) {
        return theMap.get(s);
      } else {
        final Utf8 utf8Key = new Utf8(s);
        return theMap.get(utf8Key);
      }
    } else {
      throw new UnsupportedOperationException(o.getClass().getName());
    }
  }

  @Override
  public void setProperty(final Object o, final Object o1, final Object o2)
  {
    if (o instanceof Map) {
      ((Map) o).put(o1, o2);
    } else if (o instanceof GenericRecord) {
      ((GenericRecord) o).put(String.valueOf(o1), o2);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public void removeProperty(final Object o, final Object o1)
  {
    if (o instanceof Map) {
      ((Map) o).remove(o1);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public boolean isMap(final Object o)
  {
    return o == null || o instanceof Map || o instanceof GenericRecord;
  }

  @Override
  public Object unwrap(final Object o)
  {
    return o;
  }

  private boolean isExtractableUnion(final Schema.Field field)
  {
    return field.schema().isUnion() &&
           field.schema().getTypes().stream().filter(type -> type.getType() != Schema.Type.NULL).count() > 1;
  }

  private Map<String, Object> extractUnionTypes(final Object o)
  {
    // Primitive types and unnamped complex types are keyed their type name.
    // Complex named types are keyed by their names.
    // This is safe because an Avro union can only contain a single member of each unnamed type and duplicates
    // of the same named type are not allowed. i.e only a single array is allowed, multiple records are allowed as
    // long as each has a unique name.
    // The Avro null type is elided as it's value can only ever be null
    if (o instanceof Integer) {
      return ImmutableMap.of("int", o);
    } else if (o instanceof Long) {
      return ImmutableMap.of("long", o);
    } else if (o instanceof Float) {
      return ImmutableMap.of("float", o);
    } else if (o instanceof Double) {
      return ImmutableMap.of("double", o);
    } else if (o instanceof Boolean) {
      return ImmutableMap.of("boolean", o);
    } else if (o instanceof Utf8) {
      return ImmutableMap.of("string", o);
    } else if (o instanceof ByteBuffer) {
      return ImmutableMap.of("bytes", o);
    } else if (o instanceof Map) {
      return ImmutableMap.of("map", o);
    } else if (o instanceof List) {
      return ImmutableMap.of("array", o);
    } else if (o instanceof GenericRecord) {
      return ImmutableMap.of(((GenericRecord) o).getSchema().getName(), o);
    } else if (o instanceof GenericFixed) {
      return ImmutableMap.of(((GenericFixed) o).getSchema().getName(), o);
    } else if (o instanceof GenericEnumSymbol) {
      return ImmutableMap.of(((GenericEnumSymbol<?>) o).getSchema().getName(), o);
    }
    return ImmutableMap.of();
  }
}
