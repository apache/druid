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

package org.apache.druid.data.input.impl;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;

import java.util.Collection;
import java.util.Collections;

//
//

/**
 * Custom json-path JsonProvider override to circumvent slow performance when encountering null paths as described in
 * https://github.com/json-path/JsonPath/issues/396
 *
 * Note that this only avoids errors for map properties, avoiding the exception on array paths is not possible without
 * patching json-path itself
 */
public class FastJacksonJsonNodeJsonProvider extends JacksonJsonNodeJsonProvider
{
  @Override
  public boolean isMap(Object obj)
  {
    return obj == null || super.isMap(obj);
  }

  @Override
  public Object getMapValue(Object obj, String key)
  {
    if (obj == null) {
      return null;
    } else {
      ObjectNode jsonObject = (ObjectNode) obj;
      Object o = jsonObject.get(key);
      if (!jsonObject.has(key)) {
        return null;
      } else {
        return unwrap(o);
      }
    }
  }

  @Override
  public Collection<String> getPropertyKeys(final Object o)
  {
    if (o == null) {
      return Collections.emptySet();
    }
    return super.getPropertyKeys(o);
  }
}
