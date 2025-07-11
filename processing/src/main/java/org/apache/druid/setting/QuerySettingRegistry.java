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

package org.apache.druid.setting;


import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

/**
 * Registry for query settings that can be used to configure query behavior via {@link org.apache.druid.query.QueryContext}.
 */
public class QuerySettingRegistry implements ISettingRegistry
{
  private final Map<String, SettingEntry<?>> settings = new TreeMap<>();

  private QuerySettingRegistry()
  {
    // Prevent instantiation
  }

  public Collection<SettingEntry<?>> getSettings()
  {
    return settings.values();
  }

  private static final QuerySettingRegistry INSTANCE = new QuerySettingRegistry();

  @Override
  public <T> SettingEntry<T> register(SettingEntry<T> setting)
  {
    if (settings.put(setting.name(), setting) != null) {
      throw new IllegalArgumentException("Setting with name [" + setting.name() + "] already exists.");
    }
    return setting;
  }

  public static QuerySettingRegistry getInstance()
  {
    return INSTANCE;
  }

  /**
   * Validates if the setting name is acceptable and parses the value according to the setting's type.
   * <p>
   * If the setting does not exist, {@link DruidException} is thrown.
   * If the value is not compatible with the setting's type or the value format is illegal, {@link org.apache.druid.query.BadQueryContextException} is thrown.
   *
   * @return the parsed value of the setting
   */
  public Object validateAndParse(String name, Object val)
  {
    SettingEntry<?> entry = this.settings.get(name);
    if (entry == null) {
      throw InvalidInput.exception("Setting with name [%s] does not exist.", name);
    }
    return entry.from(val);
  }
}
