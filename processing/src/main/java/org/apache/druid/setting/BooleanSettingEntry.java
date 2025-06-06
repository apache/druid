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
import org.apache.druid.query.QueryContexts;

import java.sql.Types;

public class BooleanSettingEntry extends SettingEntry<Boolean>
{
  private BooleanSettingEntry(Builder builder)
  {
    super(builder);
  }

  @Override
  public Boolean from(QueryContext context)
  {
    return QueryContexts.getAsBoolean(name, context.get(name), defaultValue);
  }

  @Override
  public Boolean from(QueryContext context, Boolean defaultValue)
  {
    return QueryContexts.getAsBoolean(name, context.get(name), defaultValue);
  }

  @Override
  public Boolean parse(Object value)
  {
    return QueryContexts.getAsBoolean(name, value);
  }

  public static class Builder extends AbstractBuilder<Boolean>
  {
    public Builder()
    {
      super(Boolean.class);
      this.defaultValue = false;
    }

    @Override
    public BooleanSettingEntry build()
    {
      return new BooleanSettingEntry(this);
    }
  }
}
