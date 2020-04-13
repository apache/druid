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

package org.apache.druid.data.input.orc;

import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.spi.json.JsonProvider;
import org.apache.orc.mapred.OrcStruct;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OrcStructJsonProvider implements JsonProvider
{
  private final OrcStructConverter converter;

  OrcStructJsonProvider(OrcStructConverter converter)
  {
    this.converter = converter;
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
  public boolean isMap(final Object o)
  {
    return o == null || o instanceof Map || o instanceof OrcStruct;
  }

  @Override
  public int length(final Object o)
  {
    if (o instanceof List) {
      return ((List) o).size();
    } else {
      return 0;
    }
  }

  @Override
  public Iterable<?> toIterable(final Object o)
  {
    if (o instanceof List) {
      return (List) o;
    }
    throw new UnsupportedOperationException(o.getClass().getName());
  }

  @Override
  public Collection<String> getPropertyKeys(final Object o)
  {
    if (o == null) {
      return Collections.emptySet();
    } else if (o instanceof Map) {
      return ((Map<Object, Object>) o).keySet().stream().map(String::valueOf).collect(Collectors.toSet());
    } else if (o instanceof OrcStruct) {
      return ((OrcStruct) o).getSchema().getFieldNames();
    } else {
      throw new UnsupportedOperationException(o.getClass().getName());
    }
  }

  @Override
  public Object getMapValue(final Object o, final String s)
  {
    if (o == null) {
      return null;
    } else if (o instanceof Map) {
      return ((Map) o).get(s);
    } else if (o instanceof OrcStruct) {
      OrcStruct struct = (OrcStruct) o;
      // get field by index since we have no way to know if this map is the root or not
      return converter.convertField(struct, struct.getSchema().getFieldNames().indexOf(s));
    }
    throw new UnsupportedOperationException(o.getClass().getName());
  }

  @Override
  public Object getArrayIndex(final Object o, final int i)
  {
    if (o instanceof List) {
      return ((List) o).get(i);
    }
    throw new UnsupportedOperationException(o.getClass().getName());
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
      throw new UnsupportedOperationException(o.getClass().getName());
    }
  }

  @Override
  public void setProperty(final Object o, final Object o1, final Object o2)
  {
    if (o instanceof Map) {
      ((Map) o).put(o1, o2);
    } else {
      throw new UnsupportedOperationException(o.getClass().getName());
    }
  }

  @Override
  public void removeProperty(final Object o, final Object o1)
  {
    if (o instanceof Map) {
      ((Map) o).remove(o1);
    } else {
      throw new UnsupportedOperationException(o.getClass().getName());
    }
  }

  @Override
  @Deprecated
  public Object getArrayIndex(final Object o, final int i, final boolean b)
  {
    throw new UnsupportedOperationException("Deprecated");
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
  public Object unwrap(final Object o)
  {
    throw new UnsupportedOperationException("Unused");
  }
}
