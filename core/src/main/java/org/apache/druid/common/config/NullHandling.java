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

package org.apache.druid.common.config;

import com.google.common.base.Strings;
import com.google.inject.Inject;

import javax.annotation.Nullable;

/**
 * Helper class for NullHandling. This class is used to switch between SQL compatible Null Handling behavior
 * introduced as part of https://github.com/apache/incubator-druid/issues/4349 and the old druid behavior
 * where null values are replaced with default values e.g Null Strings are replaced with empty values.
 */
public class NullHandling
{
  public static final String NULL_HANDLING_CONFIG_STRING = "druid.generic.useDefaultValueForNull";

  /**
   * use these values to ensure that {@link NullHandling#defaultDoubleValue()},
   * {@link NullHandling#defaultFloatValue()} , {@link NullHandling#defaultFloatValue()}
   * return the same boxed object when returning a constant zero
   */
  public static final Double ZERO_DOUBLE = 0.0d;
  public static final Float ZERO_FLOAT = 0.0f;
  public static final Long ZERO_LONG = 0L;
  public static final byte IS_NULL_BYTE = (byte) 1;
  public static final byte IS_NOT_NULL_BYTE = (byte) 0;

  /**
   * INSTANCE is injected using static injection to avoid adding JacksonInject annotations all over the code.
   * See org.apache.druid.guice.NullHandlingModule for details.
   * It does not take effect in all unit tests since we don't use Guice Injection.
   */
  @Inject
  private static NullValueHandlingConfig INSTANCE = new NullValueHandlingConfig(
      Boolean.valueOf(System.getProperty(NULL_HANDLING_CONFIG_STRING, "true"))
  );

  /**
   * whether nulls should be replaced with default value.
   */
  public static boolean replaceWithDefault()
  {
    return INSTANCE.isUseDefaultValuesForNull();
  }

  public static boolean sqlCompatible()
  {
    return !replaceWithDefault();
  }

  @Nullable
  public static String nullToEmptyIfNeeded(@Nullable String value)
  {
    //CHECKSTYLE.OFF: Regexp
    return replaceWithDefault() ? Strings.nullToEmpty(value) : value;
    //CHECKSTYLE.ON: Regexp
  }

  @Nullable
  public static String emptyToNullIfNeeded(@Nullable String value)
  {
    //CHECKSTYLE.OFF: Regexp
    return replaceWithDefault() ? Strings.emptyToNull(value) : value;
    //CHECKSTYLE.ON: Regexp
  }

  @Nullable
  public static String defaultStringValue()
  {
    return replaceWithDefault() ? "" : null;
  }

  @Nullable
  public static Long defaultLongValue()
  {
    return replaceWithDefault() ? ZERO_LONG : null;
  }

  @Nullable
  public static Float defaultFloatValue()
  {
    return replaceWithDefault() ? ZERO_FLOAT : null;
  }

  @Nullable
  public static Double defaultDoubleValue()
  {
    return replaceWithDefault() ? ZERO_DOUBLE : null;
  }

  public static boolean isNullOrEquivalent(@Nullable String value)
  {
    return replaceWithDefault() ? Strings.isNullOrEmpty(value) : value == null;
  }

}
