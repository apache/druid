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


import org.apache.druid.query.QueryContext;

public class BoundedSettingEntry<T>
{
  private final SettingEntry<T> def;
  private final QueryContext context;

  public BoundedSettingEntry(SettingEntry<T> def, QueryContext context)
  {
    this.def = def;
    this.context = context;
  }

  /**
   * Get the value using the setting's default
   */
  public T get()
  {
    return def.from(context);
  }

  /**
   * Get the value with a custom default
   */
  public T get(T defaultValue)
  {
    return def.from(context, defaultValue);
  }

  /**
   * Get the setting's name
   */
  public String name()
  {
    return def.name();
  }

  /**
   * Get the setting's default value
   */
  public T defaultValue()
  {
    return def.defaultValue();
  }
}
