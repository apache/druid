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

package org.apache.druid.java.util.metrics;

import org.apache.druid.java.util.common.logger.Logger;

import java.util.HashMap;
import java.util.Map;

public class KeyedDiff
{
  private static final Logger log = new Logger(KeyedDiff.class);

  private final Map<String, Map<String, Long>> prevs = new HashMap<String, Map<String, Long>>();

  public Map<String, Long> to(String key, Map<String, Long> curr)
  {
    final Map<String, Long> prev = prevs.put(key, curr);
    if (prev != null) {
      return subtract(curr, prev);
    } else {
      log.debug("No previous data for key[%s]", key);
      return null;
    }
  }

  public static Map<String, Long> subtract(Map<String, Long> lhs, Map<String, Long> rhs)
  {
    assert lhs.keySet().equals(rhs.keySet());
    final Map<String, Long> zs = new HashMap<>();
    for (Map.Entry<String, Long> k : lhs.entrySet()) {
      zs.put(k.getKey(), k.getValue() - rhs.get(k.getKey()));
    }
    return zs;
  }
}
