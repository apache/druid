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

package org.apache.druid.segment.filter;

import com.google.common.base.Predicate;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.filter.DruidDoublePredicate;
import org.apache.druid.query.filter.DruidFloatPredicate;
import org.apache.druid.query.filter.DruidLongPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.BaseFloatColumnValueSelector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.BaseNullableColumnValueSelector;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.DimensionSelector;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Utility methods for creating {@link ValueMatcher} instances. Mainly used by {@link ConstantValueMatcherFactory}
 * and {@link PredicateValueMatcherFactory}.
 */
public class ValueMatchers
{
  private ValueMatchers()
  {
    // No instantiation.
  }

  /**
   * Creates a constant-based {@link ValueMatcher} for a string-typed selector.
   *
   * @param selector          column selector
   * @param value             value to match
   * @param hasMultipleValues whether the column selector *might* have multiple values
   */
  public static ValueMatcher makeStringValueMatcher(
      final DimensionSelector selector,
      final String value,
      final boolean hasMultipleValues
  )
  {
    final ValueMatcher booleanMatcher = toBooleanMatcherIfPossible(
        selector,
        hasMultipleValues,
        s -> Objects.equals(s, NullHandling.emptyToNullIfNeeded(value))
    );

    if (booleanMatcher != null) {
      return booleanMatcher;
    } else {
      return selector.makeValueMatcher(value);
    }
  }

  /**
   * Creates a predicate-based {@link ValueMatcher} for a string-typed selector.
   *
   * @param selector          column selector
   * @param predicateFactory  predicate to match
   * @param hasMultipleValues whether the column selector *might* have multiple values
   */
  public static ValueMatcher makeStringValueMatcher(
      final DimensionSelector selector,
      final DruidPredicateFactory predicateFactory,
      final boolean hasMultipleValues
  )
  {
    final Predicate<String> predicate = predicateFactory.makeStringPredicate();
    final ValueMatcher booleanMatcher = toBooleanMatcherIfPossible(selector, hasMultipleValues, predicate);

    if (booleanMatcher != null) {
      return booleanMatcher;
    } else {
      return selector.makeValueMatcher(predicate);
    }
  }

  /**
   * Creates a constant-based {@link ValueMatcher} for a float-typed selector.
   *
   * @param selector column selector
   * @param value    value to match
   */
  public static ValueMatcher makeFloatValueMatcher(
      final BaseFloatColumnValueSelector selector,
      final String value
  )
  {
    final Float matchVal = DimensionHandlerUtils.convertObjectToFloat(value);
    if (matchVal == null) {
      return makeNumericNullValueMatcher(selector);
    }

    // Use "floatToIntBits" to canonicalize NaN values.
    final int matchValIntBits = Float.floatToIntBits(matchVal);
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        if (selector.isNull()) {
          return false;
        }
        return Float.floatToIntBits(selector.getFloat()) == matchValIntBits;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", selector);
      }
    };
  }

  public static ValueMatcher makeLongValueMatcher(final BaseLongColumnValueSelector selector, final String value)
  {
    final Long matchVal = DimensionHandlerUtils.convertObjectToLong(value);
    if (matchVal == null) {
      return makeNumericNullValueMatcher(selector);
    }
    final long matchValLong = matchVal;
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        if (selector.isNull()) {
          return false;
        }
        return selector.getLong() == matchValLong;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", selector);
      }
    };
  }

  public static ValueMatcher makeLongValueMatcher(
      final BaseLongColumnValueSelector selector,
      final DruidPredicateFactory predicateFactory
  )
  {
    final DruidLongPredicate predicate = predicateFactory.makeLongPredicate();
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        if (selector.isNull()) {
          return predicate.applyNull();
        }
        return predicate.applyLong(selector.getLong());
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", selector);
        inspector.visit("predicate", predicate);
      }
    };
  }

  /**
   * Creates a predicate-based {@link ValueMatcher} for a float-typed selector.
   *
   * @param selector         column selector
   * @param predicateFactory predicate to match
   */
  public static ValueMatcher makeFloatValueMatcher(
      final BaseFloatColumnValueSelector selector,
      final DruidPredicateFactory predicateFactory
  )
  {
    final DruidFloatPredicate predicate = predicateFactory.makeFloatPredicate();
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        if (selector.isNull()) {
          return predicate.applyNull();
        }
        return predicate.applyFloat(selector.getFloat());
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", selector);
        inspector.visit("predicate", predicate);
      }
    };
  }

  /**
   * Creates a constant-based {@link ValueMatcher} for a double-typed selector.
   *
   * @param selector column selector
   * @param value    value to match
   */
  public static ValueMatcher makeDoubleValueMatcher(
      final BaseDoubleColumnValueSelector selector,
      final String value
  )
  {
    final Double matchVal = DimensionHandlerUtils.convertObjectToDouble(value);
    if (matchVal == null) {
      return makeNumericNullValueMatcher(selector);
    }

    // Use "doubleToLongBits" to canonicalize NaN values.
    final long matchValLongBits = Double.doubleToLongBits(matchVal);
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        if (selector.isNull()) {
          return false;
        }
        return Double.doubleToLongBits(selector.getDouble()) == matchValLongBits;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", selector);
      }
    };
  }

  /**
   * Creates a predicate-based {@link ValueMatcher} for a double-typed selector.
   *
   * @param selector         column selector
   * @param predicateFactory predicate to match
   */
  public static ValueMatcher makeDoubleValueMatcher(
      final BaseDoubleColumnValueSelector selector,
      final DruidPredicateFactory predicateFactory
  )
  {
    final DruidDoublePredicate predicate = predicateFactory.makeDoublePredicate();
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        if (selector.isNull()) {
          return predicate.applyNull();
        }
        return predicate.applyDouble(selector.getDouble());
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", selector);
        inspector.visit("predicate", predicate);
      }
    };
  }

  /**
   * If applying {@code predicate} to {@code selector} would always return a constant, returns that constant.
   * Otherwise, returns null.
   *
   * This method would have been private, except it's also used by
   * {@link org.apache.druid.query.filter.vector.SingleValueStringVectorValueMatcher}.
   *
   * @param selector          string selector
   * @param hasMultipleValues whether the selector *might* have multiple values
   * @param predicate         predicate to apply
   */
  @Nullable
  public static Boolean toBooleanIfPossible(
      final DimensionDictionarySelector selector,
      final boolean hasMultipleValues,
      final Predicate<String> predicate
  )
  {
    if (selector.getValueCardinality() == 0) {
      // Column has no values (it doesn't exist, or it's all empty arrays).
      // Match if and only if "predicate" matches null.
      return predicate.apply(null);
    } else if (!hasMultipleValues && selector.getValueCardinality() == 1 && selector.nameLookupPossibleInAdvance()) {
      // Every row has the same value. Match if and only if "predicate" matches the possible value.
      return predicate.apply(selector.lookupName(0));
    } else {
      return null;
    }
  }

  /**
   * If {@link #toBooleanIfPossible} would return nonnull, this returns a {@link BooleanValueMatcher} that always
   * returns that value. Otherwise, this returns null.
   *
   * @param selector          string selector
   * @param hasMultipleValues whether the selector *might* have multiple values
   * @param predicate         predicate to apply
   */
  @Nullable
  private static ValueMatcher toBooleanMatcherIfPossible(
      final DimensionSelector selector,
      final boolean hasMultipleValues,
      final Predicate<String> predicate
  )
  {
    final Boolean booleanValue = ValueMatchers.toBooleanIfPossible(
        selector,
        hasMultipleValues,
        predicate
    );
    return booleanValue == null ? null : BooleanValueMatcher.of(booleanValue);
  }

  /**
   * Returns a ValueMatcher that matches when the primitive numeric (long, double, or float) value from
   * {@code selector} should be treated as null.
   */
  private static ValueMatcher makeNumericNullValueMatcher(BaseNullableColumnValueSelector selector)
  {
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        return selector.isNull();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", selector);
      }
    };
  }
}
