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

package org.apache.druid.math.expr;

import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.query.filter.DruidDoublePredicate;
import org.apache.druid.query.filter.DruidFloatPredicate;
import org.apache.druid.query.filter.DruidLongPredicate;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.index.SimpleImmutableBitmapIterableIndex;
import org.apache.druid.segment.index.semantic.DictionaryEncodedValueIndex;
import org.apache.druid.segment.index.semantic.DruidPredicateIndexes;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class ExpressionPredicateIndexSupplier implements ColumnIndexSupplier
{
  private final Expr expr;
  private final String inputColumn;
  private final ExpressionType inputType;
  private final ColumnType outputType;
  private final DictionaryEncodedValueIndex<?> inputColumnIndexes;

  public ExpressionPredicateIndexSupplier(
      Expr expr,
      String inputColumn,
      ExpressionType inputType,
      ColumnType outputType,
      DictionaryEncodedValueIndex<?> inputColumnValueIndexes
  )
  {
    this.expr = expr;
    this.inputColumn = inputColumn;
    this.inputType = inputType;
    this.outputType = outputType;
    this.inputColumnIndexes = inputColumnValueIndexes;
  }

  @Nullable
  @Override
  public <T> T as(Class<T> clazz)
  {
    if (clazz.equals(DruidPredicateIndexes.class)) {
      return (T) new ExpressionPredicateIndexes();
    }
    return null;
  }

  private final class ExpressionPredicateIndexes implements DruidPredicateIndexes
  {
    @Nullable
    @Override
    public BitmapColumnIndex forPredicate(DruidPredicateFactory matcherFactory)
    {
      final java.util.function.Function<Object, ExprEval<?>> evalFunction =
          inputValue -> expr.eval(InputBindings.forInputSupplier(inputColumn, inputType, () -> inputValue));

      return new SimpleImmutableBitmapIterableIndex()
      {
        @Override
        public Iterable<ImmutableBitmap> getBitmapIterable(boolean includeUnknown)
        {
          switch (outputType.getType()) {
            case STRING:
              return computeStringBitmaps(matcherFactory.makeStringPredicate(), evalFunction, includeUnknown);
            case LONG:
              return computeLongBitmaps(matcherFactory.makeLongPredicate(), evalFunction, includeUnknown);
            case DOUBLE:
              return computeDoubleBitmaps(matcherFactory.makeDoublePredicate(), evalFunction, includeUnknown);
            case FLOAT:
              return computeFloatBitmaps(matcherFactory.makeFloatPredicate(), evalFunction, includeUnknown);
            case ARRAY:
              return computeArrayBitmaps(matcherFactory.makeArrayPredicate(outputType), evalFunction, includeUnknown);
            default:
              return computeObjectBitmaps(matcherFactory.makeObjectPredicate(), evalFunction, includeUnknown);
          }
        }
      };
    }
  }

  private Iterable<ImmutableBitmap> computeStringBitmaps(
      DruidObjectPredicate<String> predicate,
      java.util.function.Function<Object, ExprEval<?>> evalFunction,
      boolean includeUnknown
  )
  {
    return () -> new BitmapIterator(inputColumnIndexes)
    {
      @Override
      boolean nextMatches(@Nullable Object nextValue)
      {
        return predicate.apply(evalFunction.apply(nextValue).asString())
                        .matches(includeUnknown);
      }
    };
  }

  private Iterable<ImmutableBitmap> computeLongBitmaps(
      DruidLongPredicate predicate,
      java.util.function.Function<Object, ExprEval<?>> evalFunction,
      boolean includeUnknown
  )
  {
    return () -> new BitmapIterator(inputColumnIndexes)
    {
      @Override
      boolean nextMatches(@Nullable Object nextValue)
      {
        final ExprEval<?> result = evalFunction.apply(nextValue);
        if (result.isNumericNull()) {
          return predicate.applyNull().matches(includeUnknown);
        }
        return predicate.applyLong(result.asLong()).matches(includeUnknown);
      }
    };
  }

  private Iterable<ImmutableBitmap> computeDoubleBitmaps(
      DruidDoublePredicate predicate,
      java.util.function.Function<Object, ExprEval<?>> evalFunction,
      boolean includeUnknown
  )
  {
    return () -> new BitmapIterator(inputColumnIndexes)
    {
      @Override
      boolean nextMatches(@Nullable Object nextValue)
      {
        final ExprEval<?> result = evalFunction.apply(nextValue);
        if (result.isNumericNull()) {
          return predicate.applyNull().matches(includeUnknown);
        }
        return predicate.applyDouble(result.asDouble()).matches(includeUnknown);
      }
    };
  }

  private Iterable<ImmutableBitmap> computeFloatBitmaps(
      DruidFloatPredicate predicate,
      java.util.function.Function<Object, ExprEval<?>> evalFunction,
      boolean includeUnknown
  )
  {
    return () -> new BitmapIterator(inputColumnIndexes)
    {
      @Override
      boolean nextMatches(@Nullable Object nextValue)
      {
        final ExprEval<?> result = evalFunction.apply(nextValue);
        if (result.isNumericNull()) {
          return predicate.applyNull().matches(includeUnknown);
        }
        return predicate.applyFloat((float) result.asDouble()).matches(includeUnknown);
      }
    };
  }

  private Iterable<ImmutableBitmap> computeArrayBitmaps(
      DruidObjectPredicate<Object[]> predicate,
      java.util.function.Function<Object, ExprEval<?>> evalFunction,
      boolean includeUnknown
  )
  {
    return () -> new BitmapIterator(inputColumnIndexes)
    {
      @Override
      boolean nextMatches(@Nullable Object nextValue)
      {
        final Object[] result = evalFunction.apply(nextValue).asArray();
        return predicate.apply(result).matches(includeUnknown);
      }
    };
  }

  private Iterable<ImmutableBitmap> computeObjectBitmaps(
      DruidObjectPredicate<Object> predicate,
      java.util.function.Function<Object, ExprEval<?>> evalFunction,
      boolean includeUnknown
  )
  {
    return () -> new BitmapIterator(inputColumnIndexes)
    {
      @Override
      boolean nextMatches(@Nullable Object nextValue)
      {
        final Object result = evalFunction.apply(nextValue).valueOrDefault();
        return predicate.apply(result).matches(includeUnknown);
      }
    };
  }

  private abstract static class BitmapIterator implements Iterator<ImmutableBitmap>
  {
    private final DictionaryEncodedValueIndex<?> inputColumnIndexes;
    int next;
    int index = 0;
    boolean nextSet = false;

    private BitmapIterator(DictionaryEncodedValueIndex<?> inputColumnIndexes)
    {
      this.inputColumnIndexes = inputColumnIndexes;
    }

    @Override
    public boolean hasNext()
    {
      if (!nextSet) {
        findNext();
      }
      return nextSet;
    }

    @Override
    public ImmutableBitmap next()
    {
      if (!nextSet) {
        findNext();
        if (!nextSet) {
          throw new NoSuchElementException();
        }
      }
      nextSet = false;
      return inputColumnIndexes.getBitmap(next);
    }

    private void findNext()
    {
      while (!nextSet && index < inputColumnIndexes.getCardinality()) {
        Object nextValue = inputColumnIndexes.getValue(index);
        nextSet = nextMatches(nextValue);
        if (nextSet) {
          next = index;
        }
        index++;
      }
    }

    abstract boolean nextMatches(@Nullable Object nextValue);
  }
}
