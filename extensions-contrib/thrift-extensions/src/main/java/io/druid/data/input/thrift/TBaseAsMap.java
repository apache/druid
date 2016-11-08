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

package io.druid.data.input.thrift;

import io.druid.java.util.common.logger.Logger;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class TBaseAsMap<T extends TBase> implements Map<String, Object>
{

  private static final Logger log = new Logger(TBaseAsMap.class);

  private T tBase;

  public TBaseAsMap(T tBase)
  {
    this.tBase = tBase;
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
   * When used in MapBasedRow, field in TBase will be interpret as follows:
   * <ul>
   * <li> thrift type -> druid dimension:</li>
   * <ul>
   * <li>null, boolean, i16, i32, i64, double, string, Enum, Map, Set, Struct -> String, using String.valueOf</li>
   * <li>bytes -> Arrays.toString() </li>
   * <li>List -> List&lt;String&gt;, using Lists.transform(&lt;List&gt;dimValue, TO_STRING_INCLUDING_NULL)</li>
   * </ul>
   * <li> thrift type -> druid metric:</li>
   * <ul>
   * <li>null -> 0F/0L</li>
   * <li>i16, i32, i64, double -> Float/Long, using Number.floatValue()/Number.longValue()</li>
   * <li>string -> Float/Long, using Float.valueOf()/Long.valueOf()</li>
   * <li>boolean, bytes, List, Enum, Map, Set, Struct -> ParseException</li>
   * </ul>
   * </ul>
   *
   * @param key ".".join(field names along the path)
   */
  @SuppressWarnings("unchecked")
  @Override
  public Object get(Object key)
  {
    String fieldName = key.toString();
    String[] fieldNames = fieldName.split("\\.");

    int length = fieldNames.length;
    int index = 0;
    Object ret = tBase;

    boolean fieldNotFound = false;
    while (!fieldNotFound && ret != null && index < length) {
      if (ret instanceof TBase) {
        TBase tempTBase = (TBase) ret;
        Map<? extends TFieldIdEnum, FieldMetaData> structMetaDataMap = FieldMetaData.getStructMetaDataMap(tempTBase.getClass());
        TFieldIdEnum fieldIdEnum = null;
        for (TFieldIdEnum tFieldIdEnum : structMetaDataMap.keySet()) {
          if (tFieldIdEnum.getFieldName().equals(fieldNames[index])) {
            fieldIdEnum = tFieldIdEnum;
            break;
          }
        }
        if (fieldIdEnum != null) {
          ret = tempTBase.getFieldValue(fieldIdEnum);
          index++;
        } else {
          fieldNotFound = true;
        }
      } else {
        fieldNotFound = true;
      }
    }

    if (fieldNotFound) {
      log.error("field not exists: %s", fieldName);
      return null;
    }

    if (ret instanceof byte[]) {
      ret = Arrays.toString((byte[]) ret);
    }

    return ret;
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
