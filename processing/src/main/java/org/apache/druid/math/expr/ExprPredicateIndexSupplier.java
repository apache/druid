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
import org.apache.druid.java.util.common.NonnullPair;
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

public class ExprPredicateIndexSupplier implements ColumnIndexSupplier
{
  private final Expr expr;
  private final String inputColumn;
  private final ExpressionType inputType;
  private final ColumnType outputType;
  private final DictionaryEncodedValueIndex<?> inputColumnIndexes;

  public ExprPredicateIndexSupplier(
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
      return (T) new ExprPredicateIndexes();
    }
    return null;
  }

  private final class ExprPredicateIndexes implements DruidPredicateIndexes
  {
    @Nullable
    @Override
    public BitmapColumnIndex forPredicate(DruidPredicateFactory matcherFactory)
    {
      final Supplier<NonnullPair<List<ImmutableBitmap>, List<ImmutableBitmap>>> bitmapsSupplier;
      java.util.function.Function<Object, ExprEval<?>> evalFunction =
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
          return bitmapsSupplier.get().lhs;
        }

        @Nullable
        @Override
        protected ImmutableBitmap getUnknownsBitmap()
        {
          return inputColumnIndexes.getBitmapFactory().union(bitmapsSupplier.get().rhs);
        }
      };
    }
  }

  private NonnullPair<List<ImmutableBitmap>, List<ImmutableBitmap>> computeStringBitmaps(
      DruidPredicateFactory matcherFactory,
      java.util.function.Function<Object, ExprEval<?>> evalFunction
  )
  {
    Predicate<String> predicate = matcherFactory.makeStringPredicate();

    List<ImmutableBitmap> matches = new ArrayList<>();
    List<ImmutableBitmap> unknowns = new ArrayList<>();

    for (int i = 0; i < inputColumnIndexes.getCardinality(); i++) {
      final Object inputValue = inputColumnIndexes.getValue(i);
      final String result = evalFunction.apply(inputValue).asString();
      if (result == null && matcherFactory.isNullInputUnknown()) {
        unknowns.add(inputColumnIndexes.getBitmap(i));
      } else if (predicate.apply(result)) {
        matches.add(inputColumnIndexes.getBitmap(i));
      }
    }

    return new NonnullPair<>(matches, unknowns);
  }

  private NonnullPair<List<ImmutableBitmap>, List<ImmutableBitmap>> computeLongBitmaps(
      DruidPredicateFactory matcherFactory,
      java.util.function.Function<Object, ExprEval<?>> evalFunction
  )
  {
    DruidLongPredicate predicate = matcherFactory.makeLongPredicate();

    List<ImmutableBitmap> matches = new ArrayList<>();
    List<ImmutableBitmap> unknowns = new ArrayList<>();

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

    return new NonnullPair<>(matches, unknowns);
  }

  private NonnullPair<List<ImmutableBitmap>, List<ImmutableBitmap>> computeDoubleBitmaps(
      DruidPredicateFactory matcherFactory,
      java.util.function.Function<Object, ExprEval<?>> evalFunction
  )
  {
    DruidDoublePredicate predicate = matcherFactory.makeDoublePredicate();

    List<ImmutableBitmap> matches = new ArrayList<>();
    List<ImmutableBitmap> unknowns = new ArrayList<>();

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

    return new NonnullPair<>(matches, unknowns);
  }

  private NonnullPair<List<ImmutableBitmap>, List<ImmutableBitmap>> computeFloatBitmaps(
      DruidPredicateFactory matcherFactory,
      java.util.function.Function<Object, ExprEval<?>> evalFunction
  )
  {
    DruidFloatPredicate predicate = matcherFactory.makeFloatPredicate();

    List<ImmutableBitmap> matches = new ArrayList<>();
    List<ImmutableBitmap> unknowns = new ArrayList<>();

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

    return new NonnullPair<>(matches, unknowns);
  }

  private NonnullPair<List<ImmutableBitmap>, List<ImmutableBitmap>> computeArrayBitmaps(
      DruidPredicateFactory matcherFactory,
      java.util.function.Function<Object, ExprEval<?>> evalFunction
  )
  {
    Predicate<Object[]> predicate = matcherFactory.makeArrayPredicate(outputType);

    List<ImmutableBitmap> matches = new ArrayList<>();
    List<ImmutableBitmap> unknowns = new ArrayList<>();

    for (int i = 0; i < inputColumnIndexes.getCardinality(); i++) {
      final Object inputValue = inputColumnIndexes.getValue(i);
      final Object[] result = evalFunction.apply(inputValue).asArray();
      if (result == null && matcherFactory.isNullInputUnknown()) {
        unknowns.add(inputColumnIndexes.getBitmap(i));
      } else if (predicate.apply(result)) {
        matches.add(inputColumnIndexes.getBitmap(i));
      }
    }

    return new NonnullPair<>(matches, unknowns);
  }

  private NonnullPair<List<ImmutableBitmap>, List<ImmutableBitmap>> computeComplexBitmaps(
      DruidPredicateFactory matcherFactory,
      java.util.function.Function<Object, ExprEval<?>> evalFunction
  )
  {
    Predicate<Object> predicate = matcherFactory.makeObjectPredicate();

    List<ImmutableBitmap> matches = new ArrayList<>();
    List<ImmutableBitmap> unknowns = new ArrayList<>();

    for (int i = 0; i < inputColumnIndexes.getCardinality(); i++) {
      final Object inputValue = inputColumnIndexes.getValue(i);
      final Object result = evalFunction.apply(inputValue).valueOrDefault();
      if (result == null && matcherFactory.isNullInputUnknown()) {
        unknowns.add(inputColumnIndexes.getBitmap(i));
      } else if (predicate.apply(result)) {
        matches.add(inputColumnIndexes.getBitmap(i));
      }
    }

    return new NonnullPair<>(matches, unknowns);
  }
}
