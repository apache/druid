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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.filter.DruidDoublePredicate;
import org.apache.druid.query.filter.DruidFloatPredicate;
import org.apache.druid.query.filter.DruidLongPredicate;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.DruidPredicateMatch;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.BaseFloatColumnValueSelector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.BaseNullableColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;

/**
 * Utility methods for creating {@link ValueMatcher} instances. Mainly used by {@link StringConstantValueMatcherFactory}
 * and {@link PredicateValueMatcherFactory}.
 */
public class ValueMatchers
{
  private ValueMatchers()
  {
    // No instantiation.
  }

  /**
   * Matcher for constant 'true' condition, where all rows should always match
   */
  public static ValueMatcher allTrue()
  {
    return AllTrueValueMatcher.instance();
  }

  /**
   * Matcher for constant 'false' condition, where rows will never be matched
   */
  public static ValueMatcher allFalse()
  {
    return AllFalseValueMatcher.instance();
  }

  /**
   * Matcher for constant 'unknown' condition, such as a column of all null values, where rows will never match
   * unless {@code includeUnknown} is set to true on the match function.
   */
  public static ValueMatcher allUnknown()
  {
    return AllUnknownValueMatcher.instance();
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
    final String constant = NullHandling.emptyToNullIfNeeded(value);
    final ConstantMatcherType matcherType = toConstantMatcherTypeIfPossible(
        selector,
        hasMultipleValues,
        constant == null ? DruidObjectPredicate.isNull() : DruidObjectPredicate.equalTo(constant)
    );
    if (matcherType != null) {
      return matcherType.asValueMatcher();
    }
    return selector.makeValueMatcher(value);

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
    final DruidObjectPredicate<String> predicate = predicateFactory.makeStringPredicate();
    final ConstantMatcherType constantMatcherType = toConstantMatcherTypeIfPossible(
        selector,
        hasMultipleValues,
        predicate
    );

    if (constantMatcherType != null) {
      return constantMatcherType.asValueMatcher();
    } else {
      return selector.makeValueMatcher(predicateFactory);
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

    return makeFloatValueMatcher(selector, matchVal);
  }

  /**
   * Creates a constant-based {@link ValueMatcher} for a float-typed selector.
   *
   * @param selector column selector
   * @param value    value to match
   */
  public static ValueMatcher makeFloatValueMatcher(
      final BaseFloatColumnValueSelector selector,
      final float value
  )
  {
    // Use "floatToIntBits" to canonicalize NaN values.
    final int matchValIntBits = Float.floatToIntBits(value);
    return new ValueMatcher()
    {
      @Override
      public boolean matches(boolean includeUnknown)
      {
        if (selector.isNull()) {
          return includeUnknown;
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
      public boolean matches(boolean includeUnknown)
      {
        if (selector.isNull()) {
          return predicate.applyNull().matches(includeUnknown);
        }
        return predicate.applyFloat(selector.getFloat()).matches(includeUnknown);
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
   * Creates a constant-based {@link ValueMatcher} for a long-typed selector.
   *
   * @param selector column selector
   * @param value    value to match
   */
  public static ValueMatcher makeLongValueMatcher(final BaseLongColumnValueSelector selector, final String value)
  {
    final Long matchVal = DimensionHandlerUtils.convertObjectToLong(value);
    if (matchVal == null) {
      return makeNumericNullValueMatcher(selector);
    }
    return makeLongValueMatcher(selector, matchVal);
  }

  /**
   * Creates a constant-based {@link ValueMatcher} for a long-typed selector.
   *
   * @param selector column selector
   * @param value    value to match
   */
  public static ValueMatcher makeLongValueMatcher(final BaseLongColumnValueSelector selector, long value)
  {
    return new ValueMatcher()
    {
      @Override
      public boolean matches(boolean includeUnknown)
      {
        if (selector.isNull()) {
          return includeUnknown;
        }
        return selector.getLong() == value;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", selector);
      }
    };
  }

  /**
   * Creates a predicate-based {@link ValueMatcher} for a long-typed selector.
   *
   * @param selector         column selector
   * @param predicateFactory predicate to match
   */
  public static ValueMatcher makeLongValueMatcher(
      final BaseLongColumnValueSelector selector,
      final DruidPredicateFactory predicateFactory
  )
  {
    final DruidLongPredicate predicate = predicateFactory.makeLongPredicate();
    return new ValueMatcher()
    {
      @Override
      public boolean matches(boolean includeUnknown)
      {
        if (selector.isNull()) {
          return predicate.applyNull().matches(includeUnknown);
        }
        return predicate.applyLong(selector.getLong()).matches(includeUnknown);
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

    return makeDoubleValueMatcher(selector, matchVal);
  }

  /**
   * Creates a constant-based {@link ValueMatcher} for a double-typed selector.
   *
   * @param selector column selector
   * @param value    value to match
   */
  public static ValueMatcher makeDoubleValueMatcher(
      final BaseDoubleColumnValueSelector selector,
      final double value
  )
  {
    // Use "doubleToLongBits" to canonicalize NaN values.
    final long matchValLongBits = Double.doubleToLongBits(value);
    return new ValueMatcher()
    {
      @Override
      public boolean matches(boolean includeUnknown)
      {
        if (selector.isNull()) {
          return includeUnknown;
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
      public boolean matches(boolean includeUnknown)
      {
        if (selector.isNull()) {
          return predicate.applyNull().matches(includeUnknown);
        }
        return predicate.applyDouble(selector.getDouble()).matches(includeUnknown);
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
   * Create a matcher that should always return false, except when {@code includeUnknown} is set, in which case only
   * null values will be matched. This is typically used when the filter should never match any actual values, but
   * still needs to be able to report 'unknown' matches.
   */
  public static ValueMatcher makeAlwaysFalseWithNullUnknownDimensionMatcher(final DimensionSelector selector, boolean multiValue)
  {
    final IdLookup lookup = selector.idLookup();
    // if the column doesn't have null
    if (lookup == null || !selector.nameLookupPossibleInAdvance()) {
      return new ValueMatcher()
      {
        @Override
        public boolean matches(boolean includeUnknown)
        {
          if (includeUnknown) {
            IndexedInts row = selector.getRow();
            final int size = row.size();
            if (size == 0) {
              return true;
            }
            for (int i = 0; i < size; i++) {
              if (NullHandling.isNullOrEquivalent(selector.lookupName(row.get(i)))) {
                return true;
              }
            }
          }
          return false;
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("selector", selector);
        }
      };
    } else {
      final int nullId = lookup.lookupId(null);
      if (nullId < 0) {
        // column doesn't have null value so no unknowns, can safely return always false matcher
        return ValueMatchers.allFalse();
      }
      if (multiValue) {
        return new ValueMatcher()
        {
          @Override
          public boolean matches(boolean includeUnknown)
          {
            if (includeUnknown) {
              IndexedInts row = selector.getRow();
              final int size = row.size();
              if (size == 0) {
                return true;
              }
              for (int i = 0; i < size; i++) {
                if (row.get(i) == nullId) {
                  return true;
                }
              }
            }
            return false;
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            inspector.visit("selector", selector);
          }
        };
      } else {
        return new ValueMatcher()
        {
          @Override
          public boolean matches(boolean includeUnknown)
          {
            return includeUnknown && selector.getRow().get(0) == nullId;
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            inspector.visit("selector", selector);
          }
        };
      }
    }
  }

  /**
   * Create a matcher that should always return false, except when {@code includeUnknown} is set, in which case only
   * null values will be matched. This is typically used when the filter should never match any actual values, but
   * still needs to be able to report 'unknown' matches.
   */
  public static ValueMatcher makeAlwaysFalseWithNullUnknownNumericMatcher(BaseNullableColumnValueSelector selector)
  {
    return new ValueMatcher()
    {
      @Override
      public boolean matches(boolean includeUnknown)
      {
        return includeUnknown && selector.isNull();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", selector);
      }
    };
  }

  /**
   * Create a matcher that should always return false, except when {@code includeUnknown} is set, in which case only
   * null values will be matched. This is typically used when the filter should never match any actual values, but
   * still needs to be able to report 'unknown' matches.
   */
  public static ValueMatcher makeAlwaysFalseWithNullUnknownObjectMatcher(BaseObjectColumnValueSelector<?> selector)
  {
    return new ValueMatcher()
    {
      @Override
      public boolean matches(boolean includeUnknown)
      {
        return includeUnknown && selector.getObject() == null;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", selector);
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
  public static ConstantMatcherType toConstantMatcherTypeIfPossible(
      final DimensionDictionarySelector selector,
      final boolean hasMultipleValues,
      final DruidObjectPredicate<String> predicate
  )
  {
    if (selector.getValueCardinality() == 0) {
      // Column has no values (it doesn't exist, or it's all empty arrays).
      // Match if and only if "predicate" matches null.
      final DruidPredicateMatch match = predicate.apply(null);
      if (match.matches(false)) {
        return ConstantMatcherType.ALL_TRUE;
      }
      if (match == DruidPredicateMatch.UNKNOWN) {
        return ConstantMatcherType.ALL_UNKNOWN;
      }
      return ConstantMatcherType.ALL_FALSE;
    } else if (!hasMultipleValues && selector.getValueCardinality() == 1 && selector.nameLookupPossibleInAdvance()) {
      // Every row has the same value. Match if and only if "predicate" matches the possible value.
      final String constant = selector.lookupName(0);
      final DruidPredicateMatch match = predicate.apply(constant);
      if (match == DruidPredicateMatch.TRUE) {
        return ConstantMatcherType.ALL_TRUE;
      }
      if (match == DruidPredicateMatch.UNKNOWN) {
        return ConstantMatcherType.ALL_UNKNOWN;
      }
      return ConstantMatcherType.ALL_FALSE;
    }
    return null;
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
      public boolean matches(boolean includeUnknown)
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
