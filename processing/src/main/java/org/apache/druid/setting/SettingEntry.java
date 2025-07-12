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


public abstract class SettingEntry<T>
{
  public final String name;

  /**
   * Java type class
   */
  public final Class<T> type;

  /**
   * Default value for the setting. can be null.
   */
  public final T defaultValue;

  /**
   * Minimum value for the setting. Can be null if no minimum is enforced.
   */
  public final T min;

  /**
   * Maximum value for the setting. Can be null if no maximum is enforced.
   */
  public final T max;

  /**
   * The version since which this setting is available. For example, "0.20.0".
   */
  public final String since;

  /**
   * Scope of the setting, which can be used to categorize the setting.
   * valid values:
   * "sql"
   * "native"
   * "all
   * If null, it means the setting is applicable to all scopes.
   */
  public final String scope;

  /**
   * Whether this setting is deprecated. If true, it should not be used in new queries.
   */
  public final boolean deprecated;

  /**
   * Description of the setting, which can be used to provide additional context or information.
   */
  public final String description;

  protected SettingEntry(AbstractBuilder<T> builder)
  {
    this.name = builder.name;
    this.type = builder.type;
    this.defaultValue = builder.defaultValue;
    this.min = builder.min;
    this.max = builder.max;
    this.since = builder.since;
    this.scope = builder.scope == null ? "all" : builder.scope;
    this.deprecated = builder.deprecated;
    this.description = builder.description;
  }

  /**
   * Get the value of this setting from the given context. If the setting is not present in the context, the default value of the setting is returned.
   */
  public abstract T valueOf(Object value);

  public abstract T valueOf(Object value, T defaultValue);

  @Override
  public String toString()
  {
    return name;
  }

  public abstract static class AbstractBuilder<T>
  {
    protected final Class<T> type;
    protected String name;
    protected T defaultValue;
    protected T min;
    protected T max;
    protected String scope;
    protected String since;
    protected boolean deprecated;
    protected String description;

    protected AbstractBuilder(Class<T> type)
    {
      this.type = type;
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

    public AbstractBuilder<T> min(T min)
    {
      this.min = min;
      return this;
    }

    public AbstractBuilder<T> max(T max)
    {
      this.max = max;
      return this;
    }

    public AbstractBuilder<T> range(T min, T max)
    {
      this.min = min;
      this.max = max;
      return this;
    }

    public AbstractBuilder<T> scope(String scope)
    {
      this.scope = scope;
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

    public SettingEntry<T> register(ISettingRegistry registry)
    {
      return registry.register(build());
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
