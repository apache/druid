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
import com.google.common.collect.ImmutableMap;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.query.filter.DruidDoublePredicate;
import org.apache.druid.query.filter.DruidLongPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.segment.column.ColumnIndexSupplier;
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
  private final DictionaryEncodedValueIndex<?> inputColumnIndexes;

  public ExprPredicateIndexSupplier(
      Expr expr,
      String inputColumn,
      ExpressionType inputType,
      DictionaryEncodedValueIndex<?> inputColumnValueIndexes
  )
  {
    this.expr = expr;
    this.inputColumn = inputColumn;
    this.inputType = inputType;
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
      switch (inputType.getType()) {
        case STRING:
          bitmapsSupplier = Suppliers.memoize(() -> computeStringBitmaps(matcherFactory));
          break;
        case LONG:
          bitmapsSupplier = Suppliers.memoize(() -> computeLongBitmaps(matcherFactory));
          break;
        case DOUBLE:
          bitmapsSupplier = Suppliers.memoize(() -> computeDoubleBitmaps(matcherFactory));
          break;
        case ARRAY:
          ExpressionType outputType = expr.getOutputType(
              InputBindings.inspectorFromTypeMap(ImmutableMap.of(inputColumn, inputType))
          );
          if (outputType == null) {
            // just don't use indexes if we can't compute the output type
            return null;
          }
          bitmapsSupplier = Suppliers.memoize(() -> computeArrayBitmaps(outputType, matcherFactory));
          break;
        default:
          bitmapsSupplier = Suppliers.memoize(() -> computeComplexBitmaps(matcherFactory));
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
      DruidPredicateFactory matcherFactory
  )
  {
    Predicate<String> predicate = matcherFactory.makeStringPredicate();

    List<ImmutableBitmap> matches = new ArrayList<>();
    List<ImmutableBitmap> unknowns = new ArrayList<>();

    for (int i = 0; i < inputColumnIndexes.getCardinality(); i++) {
      final Object inputValue = inputColumnIndexes.getValue(i);
      final String result = expr.eval(
          InputBindings.forInputSupplier(inputColumn, ExpressionType.STRING, () -> inputValue)
      ).asString();
      if (result == null && matcherFactory.isNullInputUnknown()) {
        unknowns.add(inputColumnIndexes.getBitmap(i));
      } else if (predicate.apply(result)) {
        matches.add(inputColumnIndexes.getBitmap(i));
      }
    }

    return new NonnullPair<>(matches, unknowns);
  }

  private NonnullPair<List<ImmutableBitmap>, List<ImmutableBitmap>> computeLongBitmaps(
      DruidPredicateFactory matcherFactory
  )
  {
    DruidLongPredicate predicate = matcherFactory.makeLongPredicate();

    List<ImmutableBitmap> matches = new ArrayList<>();
    List<ImmutableBitmap> unknowns = new ArrayList<>();

    for (int i = 0; i < inputColumnIndexes.getCardinality(); i++) {
      final Object inputValue = inputColumnIndexes.getValue(i);
      final ExprEval<?> result = expr.eval(
          InputBindings.forInputSupplier(inputColumn, ExpressionType.LONG, () -> inputValue)
      );
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
      DruidPredicateFactory matcherFactory
  )
  {
    DruidDoublePredicate predicate = matcherFactory.makeDoublePredicate();

    List<ImmutableBitmap> matches = new ArrayList<>();
    List<ImmutableBitmap> unknowns = new ArrayList<>();

    for (int i = 0; i < inputColumnIndexes.getCardinality(); i++) {
      final Object inputValue = inputColumnIndexes.getValue(i);
      final ExprEval<?> result = expr.eval(
          InputBindings.forInputSupplier(inputColumn, ExpressionType.STRING, () -> inputValue)
      );
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

  private NonnullPair<List<ImmutableBitmap>, List<ImmutableBitmap>> computeArrayBitmaps(
      ExpressionType outputType,
      DruidPredicateFactory matcherFactory
  )
  {
    Predicate<Object[]> predicate = matcherFactory.makeArrayPredicate(ExpressionType.toColumnType(outputType));

    List<ImmutableBitmap> matches = new ArrayList<>();
    List<ImmutableBitmap> unknowns = new ArrayList<>();

    for (int i = 0; i < inputColumnIndexes.getCardinality(); i++) {
      final Object inputValue = inputColumnIndexes.getValue(i);
      final Object[] result = expr.eval(
          InputBindings.forInputSupplier(inputColumn, ExpressionType.STRING, () -> inputValue)
      ).asArray();
      if (result == null && matcherFactory.isNullInputUnknown()) {
        unknowns.add(inputColumnIndexes.getBitmap(i));
      } else if (predicate.apply(result)) {
        matches.add(inputColumnIndexes.getBitmap(i));
      }
    }

    return new NonnullPair<>(matches, unknowns);
  }

  private NonnullPair<List<ImmutableBitmap>, List<ImmutableBitmap>> computeComplexBitmaps(
      DruidPredicateFactory matcherFactory
  )
  {
    Predicate<Object> predicate = matcherFactory.makeObjectPredicate();

    List<ImmutableBitmap> matches = new ArrayList<>();
    List<ImmutableBitmap> unknowns = new ArrayList<>();

    for (int i = 0; i < inputColumnIndexes.getCardinality(); i++) {
      final Object inputValue = inputColumnIndexes.getValue(i);
      final Object result = expr.eval(
          InputBindings.forInputSupplier(inputColumn, inputType, () -> inputValue)
      ).valueOrDefault();
      if (result == null && matcherFactory.isNullInputUnknown()) {
        unknowns.add(inputColumnIndexes.getBitmap(i));
      } else if (predicate.apply(result)) {
        matches.add(inputColumnIndexes.getBitmap(i));
      }
    }

    return new NonnullPair<>(matches, unknowns);
  }
}
