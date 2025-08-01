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

import org.apache.druid.query.filter.DruidDoublePredicate;
import org.apache.druid.query.filter.DruidFloatPredicate;
import org.apache.druid.query.filter.DruidLongPredicate;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.DruidPredicateMatch;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;

/**
 * {@link DruidPredicateFactory} whose predicates evaluate an {@link Expr} on the input match value, turning the result
 * of that evaluation into a {@link DruidPredicateMatch#UNKNOWN} if the value is null, and if not,
 * using {@link ExprEval#asBoolean()} to produce {@link DruidPredicateMatch#TRUE} or {@link DruidPredicateMatch#FALSE}.
 */
public class ExpressionDruidPredicateFactory implements DruidPredicateFactory
{
  private final Expr expr;
  private final ColumnCapabilities inputCapabilites;

  public ExpressionDruidPredicateFactory(Expr expr, ColumnCapabilities inputCapabilites)
  {
    this.expr = expr;
    this.inputCapabilites = inputCapabilites;
  }

  @Override
  public DruidObjectPredicate<String> makeStringPredicate()
  {
    return value -> {
      final ExprEval<?> eval = expr.eval(
          InputBindings.forInputSupplier(
              ExpressionType.STRING,
              () -> value
          )
      );
      if (eval.value() == null) {
        return DruidPredicateMatch.UNKNOWN;
      }
      return DruidPredicateMatch.of(eval.asBoolean());
    };
  }

  @Override
  public DruidLongPredicate makeLongPredicate()
  {
    return new DruidLongPredicate()
    {
      @Override
      public DruidPredicateMatch applyLong(long input)
      {
        final ExprEval<?> eval = expr.eval(InputBindings.forInputSupplier(ExpressionType.LONG, () -> input));
        if (eval.isNumericNull()) {
          return DruidPredicateMatch.UNKNOWN;
        }
        return DruidPredicateMatch.of(eval.asBoolean());
      }

      @Override
      public DruidPredicateMatch applyNull()
      {
        final ExprEval<?> eval = expr.eval(InputBindings.nilBindings());
        if (eval.isNumericNull()) {
          return DruidPredicateMatch.UNKNOWN;
        }
        return DruidPredicateMatch.of(eval.asBoolean());
      }
    };
  }

  @Override
  public DruidFloatPredicate makeFloatPredicate()
  {
    return new DruidFloatPredicate()
    {
      @Override
      public DruidPredicateMatch applyFloat(float input)
      {
        final ExprEval<?> eval = expr.eval(
            InputBindings.forInputSupplier(ExpressionType.DOUBLE, () -> input)
        );
        if (eval.isNumericNull()) {
          return DruidPredicateMatch.UNKNOWN;
        }
        return DruidPredicateMatch.of(eval.asBoolean());
      }

      @Override
      public DruidPredicateMatch applyNull()
      {

        final ExprEval<?> eval = expr.eval(InputBindings.nilBindings());
        if (eval.isNumericNull()) {
          return DruidPredicateMatch.UNKNOWN;
        }
        return DruidPredicateMatch.of(eval.asBoolean());
      }
    };
  }

  @Override
  public DruidDoublePredicate makeDoublePredicate()
  {
    return new DruidDoublePredicate()
    {
      @Override
      public DruidPredicateMatch applyDouble(double input)
      {
        final ExprEval<?> eval = expr.eval(
            InputBindings.forInputSupplier(ExpressionType.DOUBLE, () -> input)
        );
        if (eval.isNumericNull()) {
          return DruidPredicateMatch.UNKNOWN;
        }
        return DruidPredicateMatch.of(eval.asBoolean());
      }

      @Override
      public DruidPredicateMatch applyNull()
      {
        final ExprEval<?> eval = expr.eval(InputBindings.nilBindings());
        if (eval.isNumericNull()) {
          return DruidPredicateMatch.UNKNOWN;
        }
        return DruidPredicateMatch.of(eval.asBoolean());
      }
    };
  }

  @Override
  public DruidObjectPredicate<Object[]> makeArrayPredicate(@Nullable TypeSignature<ValueType> arrayType)
  {
    if (inputCapabilites == null) {
      return input -> {
        final ExprEval<?> eval = expr.eval(
            InputBindings.forInputSupplier(ExpressionType.STRING_ARRAY, () -> input)
        );
        if (eval.value() == null) {
          return DruidPredicateMatch.UNKNOWN;
        }
        return DruidPredicateMatch.of(eval.asBoolean());
      };
    }
    return input -> {
      ExprEval<?> eval = expr.eval(
          InputBindings.forInputSupplier(ExpressionType.fromColumnType(inputCapabilites), () -> input)
      );
      if (eval.value() == null) {
        return DruidPredicateMatch.UNKNOWN;
      }
      return DruidPredicateMatch.of(eval.asBoolean());
    };
  }

  // The hashcode and equals are to make SubclassesMustOverrideEqualsAndHashCodeTest stop complaining..
  // DruidPredicateFactory currently doesn't really need equals or hashcode since 'toString' method that is actually
  // called when testing equality of DimensionPredicateFilter, so it's the truly required method, but that seems
  // a bit strange. DimensionPredicateFilter should probably be reworked to use equals from DruidPredicateFactory
  // instead of using toString.
  @Override
  public int hashCode()
  {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj)
  {
    return super.equals(obj);
  }
}
