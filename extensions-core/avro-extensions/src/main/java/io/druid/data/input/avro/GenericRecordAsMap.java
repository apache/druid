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
package io.druid.data.input.avro;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import io.druid.java.util.common.StringUtils;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GenericRecordAsMap implements Map<String, Object>
{
  private final GenericRecord record;
  private final boolean fromPigAvroStorage;
  private final boolean binaryAsString;

  private static final Function<Object, String> PIG_AVRO_STORAGE_ARRAY_TO_STRING_INCLUDING_NULL = new Function<Object, String>()
  {
    @Nullable
    @Override
    public String apply(Object input)
    {
      return String.valueOf(((GenericRecord) input).get(0));
    }
  };

  public GenericRecordAsMap(GenericRecord record, boolean fromPigAvroStorage, boolean binaryAsString)
  {
    this.record = record;
    this.fromPigAvroStorage = fromPigAvroStorage;
    this.binaryAsString = binaryAsString;
  }

  @Override
  public int size()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isEmpty()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsKey(Object key)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsValue(Object value)
  {
    throw new UnsupportedOperationException();
  }

  /**
   * When used in MapBasedRow, field in GenericRecord will be interpret as follows:
   * <ul>
   * <li> avro schema type -> druid dimension:</li>
   * <ul>
   * <li>null, boolean, int, long, float, double, string, Records, Enums, Maps, Fixed -> String, using String.valueOf</li>
   * <li>bytes -> Arrays.toString() or new String if binaryAsString is true</li>
   * <li>Arrays -> List&lt;String&gt;, using Lists.transform(&lt;List&gt;dimValue, TO_STRING_INCLUDING_NULL)</li>
   * </ul>
   * <li> avro schema type -> druid metric:</li>
   * <ul>
   * <li>null -> 0F/0L</li>
   * <li>int, long, float, double -> Float/Long, using Number.floatValue()/Number.longValue()</li>
   * <li>string -> Float/Long, using Float.valueOf()/Long.valueOf()</li>
   * <li>boolean, bytes, Arrays, Records, Enums, Maps, Fixed -> ParseException</li>
   * </ul>
   * </ul>
   */
  @Override
  public Object get(Object key)
  {
    Object field = record.get(key.toString());
    if (fromPigAvroStorage && field instanceof GenericData.Array) {
      return Lists.transform((List) field, PIG_AVRO_STORAGE_ARRAY_TO_STRING_INCLUDING_NULL);
    }
    if (field instanceof ByteBuffer) {
      if (binaryAsString) {
        return StringUtils.fromUtf8(((ByteBuffer) field).array());
      } else {
        return Arrays.toString(((ByteBuffer) field).array());
      }
    }
    if (field instanceof Utf8) {
      return field.toString();
    }
    return field;
  }

  @Override
  public Object put(String key, Object value)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object remove(Object key)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putAll(Map<? extends String, ?> m)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> keySet()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<Object> values()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<Entry<String, Object>> entrySet()
  {
    throw new UnsupportedOperationException();
  }
}
