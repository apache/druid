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

import org.apache.druid.java.util.common.parsers.FlattenerJsonProvider;
import org.apache.hadoop.io.Text;
import org.apache.orc.mapred.OrcMap;
import org.apache.orc.mapred.OrcStruct;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OrcStructJsonProvider extends FlattenerJsonProvider
{
  private final OrcStructConverter converter;

  OrcStructJsonProvider(OrcStructConverter converter)
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
    return o == null || o instanceof Map || o instanceof OrcStruct;
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
    } else if (o instanceof OrcMap) {
      OrcMap map = (OrcMap) o;
      return map.get(new Text(s));
    } else if (o instanceof Map) {
      return ((Map) o).get(s);
    } else if (o instanceof OrcStruct) {
      OrcStruct struct = (OrcStruct) o;
      // get field by index since we have no way to know if this map is the root or not
      return converter.convertField(struct, struct.getSchema().getFieldNames().indexOf(s));
    }
    throw new UnsupportedOperationException(o.getClass().getName());
  }
}
