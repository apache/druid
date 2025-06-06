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

import java.util.Locale;

public abstract class SettingEntry<T>
{
  protected String name;
  protected String type;
  protected T defaultValue;
  protected boolean deprecated;
  protected String description;

  protected SettingEntry(AbstractBuilder<T> builder)
  {
    this.name = builder.name;
    this.type = builder.type;
    this.defaultValue = builder.defaultValue;
    this.deprecated = builder.deprecated;
    this.description = builder.description;
  }

  public String name()
  {
    return name;
  }

  public String type()
  {
    return type;
  }

  public T defaultValue()
  {
    return defaultValue;
  }

  public boolean deprecated()
  {
    return deprecated;
  }

  public String description()
  {
    return description;
  }

  public abstract T from(QueryContext context);

  public abstract T from(QueryContext context, T defaultValue);

  /**
   * Validate if the given value is valid for this setting.
   */
  public abstract T parse(Object value);

  public static abstract class AbstractBuilder<T>
  {
    protected final String type;
    protected String name;
    protected T defaultValue;
    protected boolean deprecated;
    protected String description;

    protected AbstractBuilder(Class<T> clazz)
    {
      this.type = clazz.getSimpleName().toLowerCase(Locale.ENGLISH);
    }

    public AbstractBuilder<T> name(String name)
    {
      this.name = name;
      return this;
    }

    public AbstractBuilder<T> defaultValue(T defaultValue)
    {
      this.defaultValue = defaultValue;
      return this;
    }

    public AbstractBuilder<T> deprecated(boolean deprecated)
    {
      this.deprecated = deprecated;
      return this;
    }

    public AbstractBuilder<T> description(String description)
    {
      this.description = description;
      return this;
    }

    public abstract SettingEntry<T> build();
  }

  public static IntegerSettingEntry.Builder newIntegerBuilder()
  {
    return new IntegerSettingEntry.Builder();
  }

  public static BooleanSettingEntry.Builder newBooleanBuilder()
  {
    return new BooleanSettingEntry.Builder();
  }

  public static FloatSettingEntry.Builder newFloatBuilder()
  {
    return new FloatSettingEntry.Builder();
  }

  public static LongSettingEntry.Builder newLongBuilder()
  {
    return new LongSettingEntry.Builder();
  }

  public static StringSettingEntry.Builder newStringBuilder()
  {
    return new StringSettingEntry.Builder();
  }

  public static ReadableNumberSetting.Builder newReadableNumberBuilder()
  {
    return new ReadableNumberSetting.Builder();
  }
}
