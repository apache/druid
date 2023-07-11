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

package org.apache.druid.data.input.parquet.simple;

import org.apache.druid.java.util.common.parsers.FlattenerJsonProvider;
import org.apache.parquet.example.data.Group;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Provides json path for Parquet {@link Group} objects
 */
public class ParquetGroupJsonProvider extends FlattenerJsonProvider
{
  private final ParquetGroupConverter converter;

  public ParquetGroupJsonProvider(ParquetGroupConverter converter)
  {
    this.converter = converter;
  }

  @Override
  public boolean isArray(final Object o)
  {
    return o instanceof List;
  }

  @Override
  public boolean isMap(final Object o)
  {
    return o == null || o instanceof Map || o instanceof Group;
  }

  @Override
  public int length(final Object o)
  {
    if (o instanceof List) {
      return ((List) o).size();
    } else if (o instanceof Group) {
      // both lists and maps are 'Group' type, but we should only have a group here in a map context
      Group g = (Group) o;
      return g.getType().getFields().size();
    } else {
      return 0;
    }
  }

  @Override
  public Collection<String> getPropertyKeys(final Object o)
  {
    if (o == null) {
      return Collections.emptySet();
    } else if (o instanceof Map) {
      return ((Map<Object, Object>) o).keySet().stream().map(String::valueOf).collect(Collectors.toSet());
    } else if (o instanceof Group) {
      return ((Group) o).getType().getFields().stream().map(f -> f.getName()).collect(Collectors.toSet());
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
    } else if (o instanceof Group) {
      Group g = (Group) o;
      return converter.convertField(g, s);
    }
    throw new UnsupportedOperationException(o.getClass().getName());
  }
}

