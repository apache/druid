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

package org.apache.druid.segment;

import org.apache.druid.java.util.common.UOE;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;

import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.Map;

public class TestColumnSelector implements ColumnSelector
{
  private final Map<String, ColumnHolder> holders = new LinkedHashMap<>();
  private final Map<String, ColumnCapabilities> capabilitiesMap = new LinkedHashMap<>();

  public TestColumnSelector addHolder(String name, ColumnHolder holder)
  {
    holders.put(name, holder);
    return this;
  }

  public TestColumnSelector addCapabilities(String name, ColumnCapabilities capability)
  {
    capabilitiesMap.put(name, capability);
    return this;
  }

  @Nullable
  @Override
  public ColumnHolder getColumnHolder(String columnName)
  {
    return getFromMap(holders, columnName, "holder");
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return getFromMap(capabilitiesMap, column, "capability");
  }

  private <T> T getFromMap(Map<String, T> map, String key, String name)
  {
    if (!map.containsKey(key)) {
      throw new UOE("%s[%s] wasn't registered, but was asked for, register first (null is okay)", name, key);
    }
    return map.get(key);
  }
}
