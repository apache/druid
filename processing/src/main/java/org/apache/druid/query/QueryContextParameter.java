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


import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.setting.SettingEntry;

public class QueryContextParameter<T>
{
  private final SettingEntry<T> entryDef;
  private final QueryContext context;

  public QueryContextParameter(SettingEntry<T> entryDef, QueryContext context)
  {
    this.entryDef = entryDef;
    this.context = context;
  }

  /**
   * Get the value using the setting's default
   */
  public T value()
  {
    T val = entryDef.valueOf(context.get(entryDef.name));
    return val == null ? null : validate(val);
  }

  /**
   * Get the value with a custom default
   */
  public T valueOrDefault(T defaultValue)
  {
    T v = entryDef.valueOf(context.get(entryDef.name), defaultValue);
    return validate(v);
  }

  /**
   * Get the setting's default value
   */
  public T defaultValue()
  {
    return entryDef.defaultValue;
  }

  private T validate(T val)
  {
    if (entryDef.min != null) {
      //noinspection unchecked
      if (((Comparable<T>) val).compareTo(entryDef.min) < 0) {
        throw new BadQueryContextException(
            StringUtils.format(
                "Parameter [%s] must be greater or equal to [%s], but is %s",
                entryDef.name,
                entryDef.min.toString(),
                val.toString()
            )
        );
      }
    }

    if (entryDef.max != null) {
      //noinspection unchecked
      if (((Comparable<T>) val).compareTo(entryDef.max) > 0) {
        throw new BadQueryContextException(
            StringUtils.format(
                "Parameter [%s] must be less or equal to [%s], but is %s",
                entryDef.name,
                entryDef.max.toString(),
                val.toString()
            )
        );
      }
    }

    return val;
  }
}
