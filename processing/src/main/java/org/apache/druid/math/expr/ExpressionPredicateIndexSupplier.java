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

import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.query.filter.DruidDoublePredicate;
import org.apache.druid.query.filter.DruidFloatPredicate;
import org.apache.druid.query.filter.DruidLongPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.index.SimpleImmutableBitmapIterableIndex;
import org.apache.druid.segment.index.semantic.DictionaryEncodedValueIndex;
import org.apache.druid.segment.index.semantic.DruidPredicateIndexes;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

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
      final Supplier<ValueAndUnknownIndexes> bitmapsSupplier;
      final java.util.function.Function<Object, ExprEval<?>> evalFunction =
          inputValue -> expr.eval(InputBindings.forInputSupplier(inputColumn, inputType, () -> inputValue));

      switch (outputType.getType()) {
        case STRING:
          bitmapsSupplier = Suppliers.memoize(() -> computeStringBitmaps(matcherFactory, evalFunction));
          break;
        case LONG:
          bitmapsSupplier = Suppliers.memoize(() -> computeLongBitmaps(matcherFactory, evalFunction));
          break;
        case DOUBLE:
          bitmapsSupplier = Suppliers.memoize(() -> computeDoubleBitmaps(matcherFactory, evalFunction));
          break;
        case FLOAT:
          bitmapsSupplier = Suppliers.memoize(() -> computeFloatBitmaps(matcherFactory, evalFunction));
          break;
        case ARRAY:
          bitmapsSupplier = Suppliers.memoize(() -> computeArrayBitmaps(matcherFactory, evalFunction));
          break;
        default:
          bitmapsSupplier = Suppliers.memoize(() -> computeComplexBitmaps(matcherFactory, evalFunction));
          break;
      }

      return new SimpleImmutableBitmapIterableIndex()
      {
        @Override
        public Iterable<ImmutableBitmap> getBitmapIterable()
        {
          return bitmapsSupplier.get().getMatches();
        }

        @Nullable
        @Override
        protected ImmutableBitmap getUnknownsBitmap()
        {
          return inputColumnIndexes.getBitmapFactory().union(bitmapsSupplier.get().getUnknowns());
        }
      };
    }
  }

  private ValueAndUnknownIndexes computeStringBitmaps(
      DruidPredicateFactory matcherFactory,
      java.util.function.Function<Object, ExprEval<?>> evalFunction
  )
  {
    final Predicate<String> predicate = matcherFactory.makeStringPredicate();
    final List<ImmutableBitmap> matches = new ArrayList<>();
    final List<ImmutableBitmap> unknowns = new ArrayList<>();

    for (int i = 0; i < inputColumnIndexes.getCardinality(); i++) {
      final Object inputValue = inputColumnIndexes.getValue(i);
      final String result = evalFunction.apply(inputValue).asString();
      if (result == null && matcherFactory.isNullInputUnknown()) {
        unknowns.add(inputColumnIndexes.getBitmap(i));
      } else if (predicate.apply(result)) {
        matches.add(inputColumnIndexes.getBitmap(i));
      }
    }

    return new ValueAndUnknownIndexes(matches, unknowns);
  }

  private ValueAndUnknownIndexes computeLongBitmaps(
      DruidPredicateFactory matcherFactory,
      java.util.function.Function<Object, ExprEval<?>> evalFunction
  )
  {
    final DruidLongPredicate predicate = matcherFactory.makeLongPredicate();
    final List<ImmutableBitmap> matches = new ArrayList<>();
    final List<ImmutableBitmap> unknowns = new ArrayList<>();

    for (int i = 0; i < inputColumnIndexes.getCardinality(); i++) {
      final Object inputValue = inputColumnIndexes.getValue(i);
      final ExprEval<?> result = evalFunction.apply(inputValue);
      if (result.isNumericNull() && matcherFactory.isNullInputUnknown()) {
        unknowns.add(inputColumnIndexes.getBitmap(i));
      } else if (result.isNumericNull() && predicate.applyNull()) {
        matches.add(inputColumnIndexes.getBitmap(i));
      } else if (!result.isNumericNull() && predicate.applyLong(result.asLong())) {
        matches.add(inputColumnIndexes.getBitmap(i));
      }
    }

    return new ValueAndUnknownIndexes(matches, unknowns);
  }

  private ValueAndUnknownIndexes computeDoubleBitmaps(
      DruidPredicateFactory matcherFactory,
      java.util.function.Function<Object, ExprEval<?>> evalFunction
  )
  {
    final DruidDoublePredicate predicate = matcherFactory.makeDoublePredicate();
    final List<ImmutableBitmap> matches = new ArrayList<>();
    final List<ImmutableBitmap> unknowns = new ArrayList<>();

    for (int i = 0; i < inputColumnIndexes.getCardinality(); i++) {
      final Object inputValue = inputColumnIndexes.getValue(i);
      final ExprEval<?> result = evalFunction.apply(inputValue);
      if (result.isNumericNull() && matcherFactory.isNullInputUnknown()) {
        unknowns.add(inputColumnIndexes.getBitmap(i));
      } else if (result.isNumericNull() && predicate.applyNull()) {
        matches.add(inputColumnIndexes.getBitmap(i));
      } else if (!result.isNumericNull() && predicate.applyDouble(result.asDouble())) {
        matches.add(inputColumnIndexes.getBitmap(i));
      }
    }

    return new ValueAndUnknownIndexes(matches, unknowns);
  }

  private ValueAndUnknownIndexes computeFloatBitmaps(
      DruidPredicateFactory matcherFactory,
      java.util.function.Function<Object, ExprEval<?>> evalFunction
  )
  {
    final DruidFloatPredicate predicate = matcherFactory.makeFloatPredicate();
    final List<ImmutableBitmap> matches = new ArrayList<>();
    final List<ImmutableBitmap> unknowns = new ArrayList<>();

    for (int i = 0; i < inputColumnIndexes.getCardinality(); i++) {
      final Object inputValue = inputColumnIndexes.getValue(i);
      final ExprEval<?> result = evalFunction.apply(inputValue);
      if (result.isNumericNull() && matcherFactory.isNullInputUnknown()) {
        unknowns.add(inputColumnIndexes.getBitmap(i));
      } else if (result.isNumericNull() && predicate.applyNull()) {
        matches.add(inputColumnIndexes.getBitmap(i));
      } else if (!result.isNumericNull() && predicate.applyFloat((float) result.asDouble())) {
        matches.add(inputColumnIndexes.getBitmap(i));
      }
    }

    return new ValueAndUnknownIndexes(matches, unknowns);
  }

  private ValueAndUnknownIndexes computeArrayBitmaps(
      DruidPredicateFactory matcherFactory,
      java.util.function.Function<Object, ExprEval<?>> evalFunction
  )
  {
    final Predicate<Object[]> predicate = matcherFactory.makeArrayPredicate(outputType);
    final List<ImmutableBitmap> matches = new ArrayList<>();
    final List<ImmutableBitmap> unknowns = new ArrayList<>();

    for (int i = 0; i < inputColumnIndexes.getCardinality(); i++) {
      final Object inputValue = inputColumnIndexes.getValue(i);
      final Object[] result = evalFunction.apply(inputValue).asArray();
      if (result == null && matcherFactory.isNullInputUnknown()) {
        unknowns.add(inputColumnIndexes.getBitmap(i));
      } else if (predicate.apply(result)) {
        matches.add(inputColumnIndexes.getBitmap(i));
      }
    }

    return new ValueAndUnknownIndexes(matches, unknowns);
  }

  private ValueAndUnknownIndexes computeComplexBitmaps(
      DruidPredicateFactory matcherFactory,
      java.util.function.Function<Object, ExprEval<?>> evalFunction
  )
  {
    final Predicate<Object> predicate = matcherFactory.makeObjectPredicate();
    final List<ImmutableBitmap> matches = new ArrayList<>();
    final List<ImmutableBitmap> unknowns = new ArrayList<>();

    for (int i = 0; i < inputColumnIndexes.getCardinality(); i++) {
      final Object inputValue = inputColumnIndexes.getValue(i);
      final Object result = evalFunction.apply(inputValue).valueOrDefault();
      if (result == null && matcherFactory.isNullInputUnknown()) {
        unknowns.add(inputColumnIndexes.getBitmap(i));
      } else if (predicate.apply(result)) {
        matches.add(inputColumnIndexes.getBitmap(i));
      }
    }

    return new ValueAndUnknownIndexes(matches, unknowns);
  }

  /**
   * Holder for two sets of {@link ImmutableBitmap}, the first set representing values that match the predicate after
   * computing the expression, and the second for values which computing the expression evaluates to null and are
   * considered 'unknown'
   */
  private static class ValueAndUnknownIndexes
  {
    private final List<ImmutableBitmap> matches;
    private final List<ImmutableBitmap> unknowns;

    private ValueAndUnknownIndexes(List<ImmutableBitmap> matches, List<ImmutableBitmap> unknowns)
    {
      this.matches = matches;
      this.unknowns = unknowns;
    }

    public List<ImmutableBitmap> getMatches()
    {
      return matches;
    }
    
    public List<ImmutableBitmap> getUnknowns()
    {
      return unknowns;
    }
  }
}
