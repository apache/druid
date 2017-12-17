/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.common.config;

import com.google.common.base.Strings;
import com.google.inject.Inject;

import javax.annotation.Nullable;

public class NullHandling
{
  private static String NULL_HANDLING_CONFIG_STRING = "druid.generic.useDefaultValueForNull";

  // use these values to ensure that convertObjectToLong(), convertObjectToDouble() and convertObjectToFloat()
  // return the same boxed object when returning a constant zero.
  public static final Double ZERO_DOUBLE = 0.0d;
  public static final Float ZERO_FLOAT = 0.0f;
  public static final Long ZERO_LONG = 0L;

  /**
   * INSTANCE is injected using static injection to avoid adding JacksonInject annotations all over the code.
   * See {@link io.druid.guice.NullHandlingModule} for details.
   * It does not take effect in all unit tests since we don't use Guice Injection.
   * For tests default system property is supposed to be used only in tests
   */
  @Inject
  private static NullValueHandlingConfig INSTANCE = new NullValueHandlingConfig(
      Boolean.valueOf(System.getProperty(NULL_HANDLING_CONFIG_STRING, "true"))
  );

  public static boolean useDefaultValuesForNull()
  {
    return INSTANCE.isUseDefaultValuesForNull();
  }

  @Nullable
  public static String nullToEmptyIfNeeded(@Nullable String value)
  {
    return useDefaultValuesForNull() ? Strings.nullToEmpty(value) : value;
  }

  @Nullable
  public static String emptyToNullIfNeeded(@Nullable String value)
  {
    return useDefaultValuesForNull() ? Strings.emptyToNull(value) : value;
  }

  public static String defaultValue()
  {
    return useDefaultValuesForNull() ? "" : null;
  }

  public static boolean isNullOrEquivalent(@Nullable String value)
  {
    return INSTANCE.isUseDefaultValuesForNull() ? Strings.isNullOrEmpty(value) : value == null;
  }

  @Nullable
  public static Long nullToZeroIfNeeded(@Nullable Long value)
  {
    return INSTANCE.isUseDefaultValuesForNull() && value == null ? ZERO_LONG : value;
  }

  @Nullable
  public static Double nullToZeroIfNeeded(@Nullable Double value)
  {
    return INSTANCE.isUseDefaultValuesForNull() && value == null ? ZERO_DOUBLE : value;
  }

  @Nullable
  public static Float nullToZeroIfNeeded(@Nullable Float value)
  {
    return INSTANCE.isUseDefaultValuesForNull() && value == null ? ZERO_FLOAT : value;
  }
}
