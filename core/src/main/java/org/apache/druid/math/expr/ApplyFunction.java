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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import it.unimi.dsi.fastutil.objects.Object2IntArrayMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.RE;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public interface ApplyFunction
{
  String name();

  ExprEval apply(LambdaExpr lambdaExpr, List<Expr> argsExpr, Expr.ObjectBinding bindings);

  Set<Expr> getArrayInputs(List<Expr> args);

  abstract class BaseMapFunction implements ApplyFunction
  {
    ExprEval applyMap(LambdaExpr expr, int length, IndexableMapLambdaObjectBinding bindings)
    {
      String[] stringsOut = null;
      Long[] longsOut = null;
      Double[] doublesOut = null;

      ExprType outputType = null;
      Object out = null;
      for (int i = 0; i < length; i++) {

        ExprEval evaluated = expr.eval(bindings.withIndex(i));
        if (outputType == null) {
          outputType = evaluated.type();
          switch (outputType) {
            case STRING:
              stringsOut = new String[length];
              out = stringsOut;
              break;
            case LONG:
              longsOut = new Long[length];
              out = longsOut;
              break;
            case DOUBLE:
              doublesOut = new Double[length];
              out = doublesOut;
              break;
            default:
              throw new RE("Unhandled map function output type [%s]", outputType);
          }
        }

        switch (outputType) {
          case STRING:
            stringsOut[i] = evaluated.asString();
            break;
          case LONG:
            longsOut[i] = evaluated.asLong();
            break;
          case DOUBLE:
            doublesOut[i] = evaluated.asDouble();
            break;
        }
      }
      return ExprEval.bestEffortOf(out);
    }
  }

  class MapFunction extends BaseMapFunction
  {
    static final String NAME = "map";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public ExprEval apply(LambdaExpr lambdaExpr, List<Expr> argsExpr, Expr.ObjectBinding bindings)
    {
      Preconditions.checkArgument(argsExpr.size() == 1);
      Expr arrayExpr = argsExpr.get(0);
      ExprEval arrayEval = arrayExpr.eval(bindings);

      Object[] array = arrayEval.asArray();
      if (array == null) {
        return ExprEval.of(null);
      }
      if (array.length == 0) {
        return arrayEval;
      }

      MapLambdaBinding lambdaBinding = new MapLambdaBinding(array, lambdaExpr, bindings);
      return applyMap(lambdaExpr, array.length, lambdaBinding);
    }

    @Override
    public Set<Expr> getArrayInputs(List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE("ApplyFunction[%s] needs 1 argument", name());
      }

      return ImmutableSet.of(args.get(0));
    }
  }

  class CartesianMapFunction extends BaseMapFunction
  {
    static final String NAME = "cartesian_map";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public ExprEval apply(LambdaExpr lambdaExpr, List<Expr> argsExpr, Expr.ObjectBinding bindings)
    {
      List<List<Object>> arrayInputs = new ArrayList<>();
      boolean hadNull = false;
      boolean hadEmpty = false;
      for (Expr expr : argsExpr) {
        ExprEval arrayEval = expr.eval(bindings);
        Object[] array = arrayEval.asArray();
        if (array == null) {
          hadNull = true;
          continue;
        }
        if (array.length == 0) {
          hadEmpty = true;
          continue;
        }
        arrayInputs.add(Arrays.asList(array));
      }
      if (hadNull) {
        return ExprEval.of(null);
      }
      if (hadEmpty) {
        return ExprEval.ofStringArray(new String[0]);
      }

      List<List<Object>> product = CartesianList.create(arrayInputs);
      CartesianMapLambdaBinding lambdaBinding = new CartesianMapLambdaBinding(product, lambdaExpr, bindings);
      return applyMap(lambdaExpr, product.size(), lambdaBinding);
    }

    @Override
    public Set<Expr> getArrayInputs(List<Expr> args)
    {
      return ImmutableSet.copyOf(args);
    }
  }

  abstract class BaseFoldFunction implements ApplyFunction
  {
    ExprEval applyFold(LambdaExpr lambdaExpr, Object accumulator, int length, IndexableFoldLambdaBinding bindings)
    {
      for (int i = 0; i < length; i++) {
        ExprEval evaluated = lambdaExpr.eval(bindings.accumulateWithIndex(i, accumulator));
        accumulator = evaluated.value();
      }
      return ExprEval.bestEffortOf(accumulator);
    }
  }

  class FoldFunction extends BaseFoldFunction
  {
    static final String NAME = "fold";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public ExprEval apply(LambdaExpr lambdaExpr, List<Expr> argsExpr, Expr.ObjectBinding bindings)
    {
      Preconditions.checkArgument(argsExpr.size() == 2);
      Expr arrayExpr = argsExpr.get(0);
      Expr accExpr = argsExpr.get(1);

      ExprEval arrayEval = arrayExpr.eval(bindings);
      ExprEval accEval = accExpr.eval(bindings);

      Object[] array = arrayEval.asArray();
      if (array == null) {
        return ExprEval.of(null);
      }
      Object accumlator = accEval.value();

      FoldLambdaBinding lambdaBinding = new FoldLambdaBinding(array, accumlator, lambdaExpr, bindings);
      return applyFold(lambdaExpr, accumlator, array.length, lambdaBinding);
    }

    @Override
    public Set<Expr> getArrayInputs(List<Expr> args)
    {
      // accumulator argument cannot be inferred, so ignore it until we think of something better to do
      return ImmutableSet.of(args.get(0));
    }
  }

  class CartesianFoldFunction extends BaseFoldFunction
  {
    static final String NAME = "cartesian_fold";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public ExprEval apply(LambdaExpr lambdaExpr, List<Expr> argsExpr, Expr.ObjectBinding bindings)
    {
      List<List<Object>> arrayInputs = new ArrayList<>();
      boolean hadNull = false;
      boolean hadEmpty = false;
      for (int i = 0; i < argsExpr.size() - 1; i++) {
        Expr expr = argsExpr.get(i);
        ExprEval arrayEval = expr.eval(bindings);
        Object[] array = arrayEval.asArray();
        if (array == null) {
          hadNull = true;
          continue;
        }
        if (array.length == 0) {
          hadEmpty = true;
          continue;
        }
        arrayInputs.add(Arrays.asList(array));
      }
      if (hadNull) {
        return ExprEval.of(null);
      }
      if (hadEmpty) {
        return ExprEval.ofStringArray(new String[0]);
      }
      Expr accExpr = argsExpr.get(argsExpr.size() - 1);

      List<List<Object>> product = CartesianList.create(arrayInputs);

      ExprEval accEval = accExpr.eval(bindings);

      Object accumlator = accEval.value();

      CartesianFoldLambdaBinding lambdaBindings =
          new CartesianFoldLambdaBinding(product, accumlator, lambdaExpr, bindings);
      return applyFold(lambdaExpr, accumlator, product.size(), lambdaBindings);
    }

    @Override
    public Set<Expr> getArrayInputs(List<Expr> args)
    {
      // accumulator argument cannot be inferred, so ignore it until we think of something better to do
      return ImmutableSet.copyOf(args.subList(0, args.size() - 1));
    }
  }

  class FilterFunction implements ApplyFunction
  {
    static final String NAME = "filter";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public ExprEval apply(LambdaExpr lambdaExpr, List<Expr> argsExpr, Expr.ObjectBinding bindings)
    {
      Preconditions.checkArgument(argsExpr.size() == 1);
      Expr arrayExpr = argsExpr.get(0);
      ExprEval arrayEval = arrayExpr.eval(bindings);

      Object[] array = arrayEval.asArray();
      if (array == null) {
        return ExprEval.of(null);
      }

      SettableLambdaBinding lambdaBinding = new SettableLambdaBinding(lambdaExpr, bindings);
      switch (arrayEval.type()) {
        case STRING:
        case STRING_ARRAY:
          String[] filteredString =
              this.filter(arrayEval.asStringArray(), lambdaExpr, lambdaBinding).toArray(String[]::new);
          return ExprEval.ofStringArray(filteredString);
        case LONG:
        case LONG_ARRAY:
          Long[] filteredLong =
              this.filter(arrayEval.asLongArray(), lambdaExpr, lambdaBinding).toArray(Long[]::new);
          return ExprEval.ofLongArray(filteredLong);
        case DOUBLE:
        case DOUBLE_ARRAY:
          Double[] filteredDouble =
              this.filter(arrayEval.asDoubleArray(), lambdaExpr, lambdaBinding).toArray(Double[]::new);
          return ExprEval.ofDoubleArray(filteredDouble);
        default:
          throw new RE("Unhandled filter function input type [%s]", arrayEval.type());
      }
    }

    @Override
    public Set<Expr> getArrayInputs(List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE("ApplyFunction[%s] needs 1 argument", name());
      }

      return ImmutableSet.of(args.get(0));
    }

    private <T> Stream<T> filter(T[] array, LambdaExpr expr, SettableLambdaBinding binding)
    {
      return Arrays.stream(array).filter(s -> expr.eval(binding.withBinding(expr.getIdentifier(), s)).asBoolean());
    }
  }

  abstract class MatchFunction implements ApplyFunction
  {
    @Override
    public ExprEval apply(LambdaExpr lambdaExpr, List<Expr> argsExpr, Expr.ObjectBinding bindings)
    {
      Preconditions.checkArgument(argsExpr.size() == 1);
      Expr arrayExpr = argsExpr.get(0);
      ExprEval arrayEval = arrayExpr.eval(bindings);

      final Object[] array = arrayEval.asArray();
      if (array == null) {
        return ExprEval.bestEffortOf(false);
      }

      SettableLambdaBinding lambdaBinding = new SettableLambdaBinding(lambdaExpr, bindings);
      return match(array, lambdaExpr, lambdaBinding);
    }

    @Override
    public Set<Expr> getArrayInputs(List<Expr> args)
    {
      if (args.size() != 1) {
        throw new IAE("ApplyFunction[%s] needs 1 argument", name());
      }

      return ImmutableSet.of(args.get(0));
    }

    public abstract ExprEval match(Object[] values, LambdaExpr expr, SettableLambdaBinding bindings);
  }

  class AnyMatchFunction extends MatchFunction
  {
    static final String NAME = "any";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public ExprEval match(Object[] values, LambdaExpr expr, SettableLambdaBinding bindings)
    {
      boolean anyMatch = Arrays.stream(values)
                               .anyMatch(o -> expr.eval(bindings.withBinding(expr.getIdentifier(), o)).asBoolean());
      return ExprEval.bestEffortOf(anyMatch);
    }
  }

  class AllMatchFunction extends MatchFunction
  {
    static final String NAME = "all";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public ExprEval match(Object[] values, LambdaExpr expr, SettableLambdaBinding bindings)
    {
      boolean allMatch = Arrays.stream(values)
                               .allMatch(o -> expr.eval(bindings.withBinding(expr.getIdentifier(), o)).asBoolean());
      return ExprEval.bestEffortOf(allMatch);
    }
  }

  class SettableLambdaBinding implements Expr.ObjectBinding
  {
    private final Expr.ObjectBinding bindings;
    private final Map<String, Object> lambdaBindings;

    SettableLambdaBinding(LambdaExpr expr, Expr.ObjectBinding bindings)
    {
      this.lambdaBindings = new HashMap<>();
      for (String lambdaIdentifier : expr.getIdentifiers()) {
        lambdaBindings.put(lambdaIdentifier, null);
      }
      this.bindings = bindings;
    }

    @Nullable
    @Override
    public Object get(String name)
    {
      if (lambdaBindings.containsKey(name)) {
        return lambdaBindings.get(name);
      }
      return bindings.get(name);
    }

    SettableLambdaBinding withBinding(String key, Object value)
    {
      this.lambdaBindings.put(key, value);
      return this;
    }
  }

  interface IndexableMapLambdaObjectBinding extends Expr.ObjectBinding
  {
    IndexableMapLambdaObjectBinding withIndex(int index);
  }

  class MapLambdaBinding implements IndexableMapLambdaObjectBinding
  {
    private final Expr.ObjectBinding bindings;
    private final String lambdaIdentifier;
    private final Object[] arrayValues;
    private int index = 0;

    MapLambdaBinding(Object[] arrayValues, LambdaExpr expr, Expr.ObjectBinding bindings)
    {
      this.lambdaIdentifier = expr.getIdentifier();
      this.arrayValues = arrayValues;
      this.bindings = bindings;
    }

    @Nullable
    @Override
    public Object get(String name)
    {
      if (name.equals(lambdaIdentifier)) {
        return arrayValues[index];
      }
      return bindings.get(name);
    }

    @Override
    public MapLambdaBinding withIndex(int index)
    {
      this.index = index;
      return this;
    }
  }

  class CartesianMapLambdaBinding implements IndexableMapLambdaObjectBinding
  {
    private final Expr.ObjectBinding bindings;
    private final Object2IntMap<String> lambdaIdentifiers;
    private final List<List<Object>> lambdaInputs;
    private int index = 0;

    CartesianMapLambdaBinding(List<List<Object>> inputs, LambdaExpr expr, Expr.ObjectBinding bindings)
    {
      this.lambdaInputs = inputs;
      List<String> ids = expr.getIdentifiers();
      this.lambdaIdentifiers = new Object2IntArrayMap<>(ids.size());
      for (int i = 0; i < ids.size(); i++) {
        lambdaIdentifiers.put(ids.get(i), i);
      }

      this.bindings = bindings;
    }

    @Nullable
    @Override
    public Object get(String name)
    {
      if (lambdaIdentifiers.containsKey(name)) {
        return lambdaInputs.get(index).get(lambdaIdentifiers.getInt(name));
      }
      return bindings.get(name);
    }

    @Override
    public CartesianMapLambdaBinding withIndex(int index)
    {
      this.index = index;
      return this;
    }
  }

  interface IndexableFoldLambdaBinding extends Expr.ObjectBinding
  {
    IndexableFoldLambdaBinding accumulateWithIndex(int index, Object accumulator);
  }

  class FoldLambdaBinding implements IndexableFoldLambdaBinding
  {
    private final Expr.ObjectBinding bindings;
    private final String elementIdentifier;
    private final Object[] arrayValues;
    private final String accumulatorIdentifier;
    private Object accumulatorValue;
    private int index;

    FoldLambdaBinding(Object[] arrayValues, Object initialAccumulator, LambdaExpr expr, Expr.ObjectBinding bindings)
    {
      List<String> ids = expr.getIdentifiers();
      this.elementIdentifier = ids.get(0);
      this.accumulatorIdentifier = ids.get(1);
      this.arrayValues = arrayValues;
      this.accumulatorValue = initialAccumulator;
      this.bindings = bindings;
    }

    @Nullable
    @Override
    public Object get(String name)
    {
      if (name.equals(elementIdentifier)) {
        return arrayValues[index];
      } else if (name.equals(accumulatorIdentifier)) {
        return accumulatorValue;
      }
      return bindings.get(name);
    }

    @Override
    public FoldLambdaBinding accumulateWithIndex(int index, Object acc)
    {
      this.index = index;
      this.accumulatorValue = acc;
      return this;
    }
  }

  class CartesianFoldLambdaBinding implements IndexableFoldLambdaBinding
  {
    private final Expr.ObjectBinding bindings;
    private final Object2IntMap<String> lambdaIdentifiers;
    private final List<List<Object>> lambdaInputs;
    private final String accumulatorIdentifier;
    private Object accumulatorValue;
    private int index = 0;

    CartesianFoldLambdaBinding(List<List<Object>> inputs, Object accumulatorValue, LambdaExpr expr, Expr.ObjectBinding bindings)
    {
      this.lambdaInputs = inputs;
      List<String> ids = expr.getIdentifiers();
      this.lambdaIdentifiers = new Object2IntArrayMap<>(ids.size());
      for (int i = 0; i < ids.size() - 1; i++) {
        lambdaIdentifiers.put(ids.get(i), i);
      }
      this.accumulatorIdentifier = ids.get(ids.size() - 1);
      this.bindings = bindings;
      this.accumulatorValue = accumulatorValue;
    }

    @Nullable
    @Override
    public Object get(String name)
    {
      if (lambdaIdentifiers.containsKey(name)) {
        return lambdaInputs.get(index).get(lambdaIdentifiers.getInt(name));
      } else if (accumulatorIdentifier.equals(name)) {
        return accumulatorValue;
      }
      return bindings.get(name);
    }

    @Override
    public CartesianFoldLambdaBinding accumulateWithIndex(int index, Object acc)
    {
      this.index = index;
      this.accumulatorValue = acc;
      return this;
    }
  }
}
