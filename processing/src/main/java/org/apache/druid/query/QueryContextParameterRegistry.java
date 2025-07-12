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

package org.apache.druid.query;


import org.apache.druid.setting.ISettingRegistry;
import org.apache.druid.setting.SettingEntry;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

/**
 * Registry for query settings that can be used to configure query behavior via {@link org.apache.druid.query.QueryContext}.
 */
public class QueryContextParameterRegistry implements ISettingRegistry
{
  private final Map<String, SettingEntry<?>> parameters = new TreeMap<>();

  private QueryContextParameterRegistry()
  {
    // Prevent instantiation
  }

  public Collection<SettingEntry<?>> getParameters()
  {
    return parameters.values();
  }

  private static final QueryContextParameterRegistry INSTANCE = new QueryContextParameterRegistry();

  @Override
  public <T> SettingEntry<T> register(SettingEntry<T> parameter)
  {
    if (parameters.put(parameter.name(), parameter) != null) {
      throw new IllegalArgumentException("Setting with name [" + parameter.name() + "] already exists.");
    }
    return parameter;
  }

  public static QueryContextParameterRegistry getInstance()
  {
    return INSTANCE;
  }
}
