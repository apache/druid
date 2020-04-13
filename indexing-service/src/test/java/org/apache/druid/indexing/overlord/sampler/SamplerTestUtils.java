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

package org.apache.druid.indexing.overlord.sampler;

import org.apache.druid.java.util.common.ISE;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class SamplerTestUtils
{
  public static class MapAllowingNullValuesBuilder<K, V>
  {
    private final Map<K, V> map = new HashMap<>();

    public MapAllowingNullValuesBuilder<K, V> put(K key, V val)
    {
      if (map.put(key, val) != null) {
        throw new ISE("Duplicate key[%s]", key);
      }
      return this;
    }

    public Map<K, V> build()
    {
      return Collections.unmodifiableMap(map);
    }
  }

  private SamplerTestUtils()
  {
  }
}
