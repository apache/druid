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

  /**
   * Java type name of the setting
   */
  protected String type;

  /**
   * Default value for the setting. can be null.
   */
  protected T defaultValue;

  /**
   * The version since which this setting is available. For example, "0.20.0".
   */
  protected String since;

  /**
   * Whether this setting is deprecated. If true, it should not be used in new queries.
   */
  protected boolean deprecated;

  /**
   * Description of the setting, which can be used to provide additional context or information.
   */
  protected String description;

  protected SettingEntry(AbstractBuilder<T> builder)
  {
    this.name = builder.name;
    this.type = builder.type;
    this.defaultValue = builder.defaultValue;
    this.since = builder.since;
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

  /**
   * Get the value of this setting from the given context. If the setting is not present in the context, the default value of the setting is returned.
   */
  public abstract T from(QueryContext context);

  public abstract T from(QueryContext context, T defaultValue);

  /**
   * Convert the given value to the type of this setting.
   */
  public abstract T convert(Object value);

  public static abstract class AbstractBuilder<T>
  {
    protected final String type;
    protected String name;
    protected T defaultValue;
    protected String since;
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

    public AbstractBuilder<T> since(String since)
    {
      this.since = since;
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

  public static IntegerSettingEntry.Builder newIntegerEntry()
  {
    return new IntegerSettingEntry.Builder();
  }

  public static BooleanSettingEntry.Builder newBooleanEntry()
  {
    return new BooleanSettingEntry.Builder();
  }

  public static FloatSettingEntry.Builder newFloatEntry()
  {
    return new FloatSettingEntry.Builder();
  }

  public static LongSettingEntry.Builder newLongEntry()
  {
    return new LongSettingEntry.Builder();
  }

  public static StringSettingEntry.Builder newStringEntry()
  {
    return new StringSettingEntry.Builder();
  }

  public static ReadableNumberSetting.Builder newReadableNumberEntry()
  {
    return new ReadableNumberSetting.Builder();
  }
}
