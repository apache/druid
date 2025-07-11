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


import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.query.QueryContexts;

public class ReadableNumberSetting extends SettingEntry<HumanReadableBytes>
{
  private ReadableNumberSetting(Builder builder)
  {
    super(builder);
  }

  @Override
  public HumanReadableBytes from(Object value)
  {
    return QueryContexts.getAsHumanReadableBytes(name, value, defaultValue);
  }

  @Override
  public HumanReadableBytes from(Object value, HumanReadableBytes defaultValue)
  {
    return QueryContexts.getAsHumanReadableBytes(name, value, defaultValue);
  }

  public static class Builder extends AbstractBuilder<HumanReadableBytes>
  {
    public Builder()
    {
      super(HumanReadableBytes.class);
      this.defaultValue = HumanReadableBytes.ZERO;
    }

    @Override
    public ReadableNumberSetting build()
    {
      return new ReadableNumberSetting(this);
    }
  }
}
