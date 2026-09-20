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

package org.apache.druid.segment.index;

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.error.DruidException;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.filter.DruidDoublePredicate;
import org.apache.druid.query.filter.DruidFloatPredicate;
import org.apache.druid.query.filter.DruidLongPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.DruidPredicateMatch;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.index.semantic.DruidPredicateIndexes;
import org.apache.druid.segment.index.semantic.NullValueIndex;
import org.apache.druid.segment.index.semantic.ValueIndexes;
import org.apache.druid.segment.index.semantic.ValueSetIndexes;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * Index supplier for a primitive column whose value is the same for every row. All indexes produced by this class
 * are either {@link AllTrueBitmapColumnIndex}, {@link AllFalseBitmapColumnIndex},
 * or {@link AllUnknownBitmapColumnIndex}.
 */
public class ConstantColumnIndexSupplier implements ColumnIndexSupplier
{
  /**
   * The constant value, normalized to {@link #type}.
   */
  @Nullable
  private final Object value;

  /**
   * Type of {@link #value}.
   */
  private final ColumnType type;

  /**
   * Expression type equivalent of {@link #type}.
   */
  private final ExpressionType exprType;

  private final int numRows;
  private final BitmapFactory bitmapFactory;

  public ConstantColumnIndexSupplier(
      final ColumnType type,
      @Nullable final Object value,
      final int numRows,
      final BitmapFactory bitmapFactory
  )
  {
    if (!type.is(ValueType.STRING) && !type.isNumeric()) {
      throw DruidException.defensive("Cannot handle type[%s]", type);
    }
    this.type = type;
    this.numRows = numRows;
    this.bitmapFactory = bitmapFactory;
    this.exprType = ExpressionType.fromColumnTypeStrict(type);

    // Normalize the value to the expected type.
    final ExprEval<?> constant = ExprEval.bestEffortOf(value).castTo(exprType);
    if (constant.value() == null) {
      this.value = null;
    } else {
      // FLOAT has no ExpressionType of its own, so explicitly narrow it back down.
      this.value = type.is(ValueType.FLOAT) ? (float) constant.asDouble() : constant.value();
    }
  }

  @Nullable
  @Override
  @SuppressWarnings("unchecked")
  public <T> T as(Class<T> clazz)
  {
    if (clazz.equals(NullValueIndex.class)) {
      final BitmapColumnIndex nullIndex = value == null ? allTrue() : allFalse();
      return (T) (NullValueIndex) () -> nullIndex;
    } else if (clazz.equals(ValueIndexes.class)) {
      return (T) new ConstantValueIndexes();
    } else if (clazz.equals(ValueSetIndexes.class)) {
      return (T) new ConstantValueSetIndexes();
    } else if (clazz.equals(DruidPredicateIndexes.class)) {
      return (T) new ConstantDruidPredicateIndexes();
    }
    return null;
  }

  private BitmapColumnIndex allTrue()
  {
    return new AllTrueBitmapColumnIndex(bitmapFactory, numRows);
  }

  private BitmapColumnIndex allFalse()
  {
    return new AllFalseBitmapColumnIndex(bitmapFactory);
  }

  private BitmapColumnIndex allUnknown()
  {
    return new AllUnknownBitmapColumnIndex(bitmapFactory, numRows);
  }

  /**
   * Whether some incoming value is equal to {@link #value}.
   */
  private boolean matchesConstant(@Nullable Object matchValue, TypeSignature<ValueType> matchValueType)
  {
    if (matchValue == null) {
      return value == null;
    }
    if (value == null) {
      return false;
    }
    final ExprEval<?> eval = ExprEval.ofType(ExpressionType.fromColumnTypeStrict(matchValueType), matchValue);
    final ExprEval<?> castForComparison = ExprEval.castForEqualityComparison(eval, exprType);
    if (castForComparison == null || castForComparison.value() == null) {
      return false;
    }
    return switch (type.getType()) {
      case STRING -> value.equals(castForComparison.asString());
      case LONG -> (Long) value == castForComparison.asLong();
      // compare bits instead of == to canonicalize NaN
      case FLOAT -> Float.floatToIntBits((Float) value) == Float.floatToIntBits((float) castForComparison.asDouble());
      case DOUBLE -> Double.doubleToLongBits((Double) value) == Double.doubleToLongBits(castForComparison.asDouble());
      default -> throw DruidException.defensive("Cannot match values for type[%s]", type);
    };
  }

  private class ConstantValueIndexes implements ValueIndexes
  {
    @Nullable
    @Override
    public BitmapColumnIndex forValue(@Nonnull Object matchValue, TypeSignature<ValueType> matchValueType)
    {
      if (!matchValueType.isPrimitive()) {
        return null;
      } else if (value == null) {
        return allUnknown();
      } else {
        return matchesConstant(matchValue, matchValueType) ? allTrue() : allFalse();
      }
    }
  }

  private class ConstantValueSetIndexes implements ValueSetIndexes
  {
    @Nullable
    @Override
    public BitmapColumnIndex forSortedValues(@Nonnull List<?> sortedValues, TypeSignature<ValueType> matchValueType)
    {
      if (!matchValueType.isPrimitive()) {
        return null;
      }

      if (sortedValues.isEmpty()) {
        return allFalse();
      }

      if (matchValueType.getType() == type.getType()) {
        if (Collections.binarySearch(sortedValues, value, matchValueType.getNullableStrategy()) >= 0) {
          return allTrue();
        }
      } else {
        for (final Object matchValue : sortedValues) {
          if (matchesConstant(matchValue, matchValueType)) {
            return allTrue();
          }
        }
      }

      return value == null ? allUnknown() : allFalse();
    }
  }

  private class ConstantDruidPredicateIndexes implements DruidPredicateIndexes
  {
    @Override
    public BitmapColumnIndex forPredicate(DruidPredicateFactory matcherFactory)
    {
      final DruidPredicateMatch match;
      switch (type.getType()) {
        case STRING: {
          match = matcherFactory.makeStringPredicate().apply((String) value);
          break;
        }
        case LONG: {
          final DruidLongPredicate predicate = matcherFactory.makeLongPredicate();
          match = value == null ? predicate.applyNull() : predicate.applyLong((Long) value);
          break;
        }
        case FLOAT: {
          final DruidFloatPredicate predicate = matcherFactory.makeFloatPredicate();
          match = value == null ? predicate.applyNull() : predicate.applyFloat((Float) value);
          break;
        }
        case DOUBLE: {
          final DruidDoublePredicate predicate = matcherFactory.makeDoublePredicate();
          match = value == null ? predicate.applyNull() : predicate.applyDouble((Double) value);
          break;
        }
        default:
          throw DruidException.defensive("Cannot apply predicates for type[%s]", type);
      }
      return switch (match) {
        case TRUE -> allTrue();
        case FALSE -> allFalse();
        case UNKNOWN -> allUnknown();
      };
    }
  }
}
