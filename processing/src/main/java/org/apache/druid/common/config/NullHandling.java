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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.filter.vector.ReadableVectorMatch;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.filter.NotFilter;
import org.apache.druid.segment.index.BitmapColumnIndex;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * Helper class for NullHandling. This class is used to switch between SQL compatible Null Handling behavior
 * introduced as part of https://github.com/apache/druid/issues/4349 and the old druid behavior
 * where null values are replaced with default values e.g Null Strings are replaced with empty values.
 */
public class NullHandling
{
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
  private static NullValueHandlingConfig INSTANCE;

  /**
   * Many unit tests do not setup modules for this value to be injected, this method provides a manual way to initialize
   * {@link #INSTANCE}
   */
  @VisibleForTesting
  public static void initializeForTests()
  {
    INSTANCE = new NullValueHandlingConfig(null, null, null);
  }

  @VisibleForTesting
  public static void initializeForTestsWithValues(Boolean useDefForNull, Boolean ignoreNullForString)
  {
    initializeForTestsWithValues(useDefForNull, null, ignoreNullForString);
  }

  @VisibleForTesting
  public static void initializeForTestsWithValues(
      Boolean useDefForNull,
      Boolean useThreeValueLogic,
      Boolean ignoreNullForString
  )
  {
    INSTANCE = new NullValueHandlingConfig(useDefForNull, useThreeValueLogic, ignoreNullForString);
  }

  /**
   * whether nulls should be replaced with default value.
   */
  public static boolean replaceWithDefault()
  {
    // this should only be null in a unit test context, in production this will be injected by the null handling module
    if (INSTANCE == null) {
      throw new IllegalStateException("NullHandling module not initialized, call NullHandling.initializeForTests()");
    }
    return INSTANCE.isUseDefaultValuesForNull();
  }

  /**
   * whether nulls should be counted during String cardinality
   */
  public static boolean ignoreNullsForStringCardinality()
  {
    // this should only be null in a unit test context, in production this will be injected by the null handling module
    if (INSTANCE == null) {
      throw new IllegalStateException("NullHandling module not initialized, call NullHandling.initializeForTests()");
    }
    return INSTANCE.isIgnoreNullsForStringCardinality();
  }

  public static boolean sqlCompatible()
  {
    return !replaceWithDefault();
  }

  /**
   * Whether filtering uses 3-valued logic. Used primarily by {@link NotFilter} to invert matches in a SQL compliant
   * manner. When this is set, an "includeUnknown" parameter can be activated in various classes related to filtering;
   * see below for references.
   *
   * @see ValueMatcher#matches(boolean) includeUnknown parameter
   * @see VectorValueMatcher#match(ReadableVectorMatch, boolean) includeUnknown parameter
   * @see BitmapColumnIndex#computeBitmapResult(BitmapResultFactory, boolean) includeUnknown parameter
   * @see DimFilter#optimize(boolean) mayIncludeUnknown parameter
   */
  public static boolean useThreeValueLogic()
  {
    return NullHandling.sqlCompatible() &&
           INSTANCE.isUseThreeValueLogicForNativeFilters() &&
           ExpressionProcessing.useStrictBooleans();
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

  /**
   * Returns the default value for an object of the provided class. Will be null in SQL-compatible null handling mode.
   * May be null or some non-null default value when not in SQL-compatible null handling mode.
   */
  @Nullable
  @SuppressWarnings("unchecked")
  public static <T> T defaultValueForClass(final Class<T> clazz)
  {
    if (clazz == Float.class) {
      return (T) defaultFloatValue();
    } else if (clazz == Double.class) {
      return (T) defaultDoubleValue();
    } else if (clazz == Long.class) {
      return (T) defaultLongValue();
    } else if (clazz == Number.class) {
      return (T) defaultDoubleValue();
    } else if (clazz == String.class) {
      return (T) defaultStringValue();
    } else {
      return null;
    }
  }

  /**
   * Returns the default value for the given {@link ValueType}.
   *
   * May be null or non-null based on the current SQL-compatible null handling mode.
   */
  @Nullable
  public static Object defaultValueForType(ValueType type)
  {
    if (sqlCompatible()) {
      return null;
    } else if (type == ValueType.FLOAT) {
      return defaultFloatValue();
    } else if (type == ValueType.DOUBLE) {
      return defaultDoubleValue();
    } else if (type == ValueType.LONG) {
      return defaultLongValue();
    } else if (type == ValueType.STRING) {
      return defaultStringValue();
    } else {
      return null;
    }
  }

  public static boolean isNullOrEquivalent(@Nullable String value)
  {
    return replaceWithDefault() ? Strings.isNullOrEmpty(value) : value == null;
  }

  public static boolean isNullOrEquivalent(@Nullable ByteBuffer buffer)
  {
    return buffer == null || (replaceWithDefault() && buffer.remaining() == 0);
  }

  /**
   * Given a UTF-8 dictionary, returns whether the first two entries must be coalesced into a single null entry.
   * This happens if we are in default-value mode and the first two entries are null and empty string.
   *
   * This and {@link #mustReplaceFirstValueWithNullInDictionary(Indexed)} are never both true.
   *
   * Provided to enable compatibility for segments written under {@link #sqlCompatible()} mode but
   * read under {@link #replaceWithDefault()} mode.
   */
  public static boolean mustCombineNullAndEmptyInDictionary(final Indexed<ByteBuffer> dictionaryUtf8)
  {
    return NullHandling.replaceWithDefault()
           && dictionaryUtf8.size() >= 2
           && isNullOrEquivalent(dictionaryUtf8.get(0))
           && isNullOrEquivalent(dictionaryUtf8.get(1));
  }

  /**
   * Given a UTF-8 dictionary, returns whether the first entry must be replaced with null. This happens if we
   * are in default-value mode and the first entry is an empty string. (Default-value mode expects it to be null.)
   *
   * This and {@link #mustCombineNullAndEmptyInDictionary(Indexed)} are never both true.
   *
   * Provided to enable compatibility for segments written under {@link #sqlCompatible()} mode but
   * read under {@link #replaceWithDefault()} mode.
   */
  public static boolean mustReplaceFirstValueWithNullInDictionary(final Indexed<ByteBuffer> dictionaryUtf8)
  {
    if (NullHandling.replaceWithDefault() && dictionaryUtf8.size() >= 1) {
      final ByteBuffer firstValue = dictionaryUtf8.get(0);
      return firstValue != null && firstValue.remaining() == 0;
    }

    return false;
  }
}
