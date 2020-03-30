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
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Base interface describing the mechanism used to evaluate an {@link ApplyFunctionExpr}, which 'applies' a
 * {@link LambdaExpr} to one or more array {@link Expr}.  All {@link ApplyFunction} implementations are immutable.
 */
public interface ApplyFunction
{
  /**
   * Name of the function
   */
  String name();

  /**
   * Apply {@link LambdaExpr} to argument list of {@link Expr} given a set of outer {@link Expr.ObjectBinding}. These
   * outer bindings will be used to form the scope for the bindings used to evaluate the {@link LambdaExpr}, which use
   * the array inputs to supply scalar values to use as bindings for {@link IdentifierExpr} in the lambda body.
   */
  ExprEval apply(LambdaExpr lambdaExpr, List<Expr> argsExpr, Expr.ObjectBinding bindings);

  /**
   * Get list of input arguments which must evaluate to an array {@link ExprType}
   */
  Set<Expr> getArrayInputs(List<Expr> args);

  /**
   * Returns true if apply function produces an array output. All {@link ApplyFunction} implementations are expected to
   * exclusively produce either scalar or array values.
   */
  default boolean hasArrayOutput(LambdaExpr lambdaExpr)
  {
    return false;
  }

  /**
   * Validate apply function arguments, throwing an exception if incorrect
   */
  void validateArguments(LambdaExpr lambdaExpr, List<Expr> args);

  /**
   * Base class for "map" functions, which are a class of {@link ApplyFunction} which take a lambda function that is
   * mapped to the values of an {@link IndexableMapLambdaObjectBinding} which is created from the outer
   * {@link Expr.ObjectBinding} and the values of the array {@link Expr} argument(s)
   */
  abstract class BaseMapFunction implements ApplyFunction
  {
    @Override
    public boolean hasArrayOutput(LambdaExpr lambdaExpr)
    {
      return true;
    }

    /**
     * Evaluate {@link LambdaExpr} against every index position of an {@link IndexableMapLambdaObjectBinding}
     */
    ExprEval applyMap(LambdaExpr expr, IndexableMapLambdaObjectBinding bindings)
    {
      final int length = bindings.getLength();
      String[] stringsOut = null;
      Long[] longsOut = null;
      Double[] doublesOut = null;

      ExprType elementType = null;
      for (int i = 0; i < length; i++) {

        ExprEval evaluated = expr.eval(bindings.withIndex(i));
        if (elementType == null) {
          elementType = evaluated.type();
          switch (elementType) {
            case STRING:
              stringsOut = new String[length];
              break;
            case LONG:
              longsOut = new Long[length];
              break;
            case DOUBLE:
              doublesOut = new Double[length];
              break;
            default:
              throw new RE("Unhandled map function output type [%s]", elementType);
          }
        }

        Function.ArrayConstructorFunction.setArrayOutputElement(
            stringsOut,
            longsOut,
            doublesOut,
            elementType,
            i,
            evaluated
        );
      }

      switch (elementType) {
        case STRING:
          return ExprEval.ofStringArray(stringsOut);
        case LONG:
          return ExprEval.ofLongArray(longsOut);
        case DOUBLE:
          return ExprEval.ofDoubleArray(doublesOut);
        default:
          throw new RE("Unhandled map function output type [%s]", elementType);
      }
    }
  }

  /**
   * Map the scalar values of a single array input {@link Expr} to a single argument {@link LambdaExpr}
   */
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
      return applyMap(lambdaExpr, lambdaBinding);
    }

    @Override
    public Set<Expr> getArrayInputs(List<Expr> args)
    {
      if (args.size() == 1) {
        return ImmutableSet.of(args.get(0));
      }
      return Collections.emptySet();
    }

    @Override
    public void validateArguments(LambdaExpr lambdaExpr, List<Expr> args)
    {
      Preconditions.checkArgument(args.size() == 1);
      if (lambdaExpr.identifierCount() > 0) {
        Preconditions.checkArgument(
            args.size() == lambdaExpr.identifierCount(),
            StringUtils.format("lambda expression argument count does not match %s argument count", name())
        );
      }
    }
  }

  /**
   * Map the cartesian product of 'n' array input arguments to an 'n' argument {@link LambdaExpr}
   */
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
      return applyMap(lambdaExpr, lambdaBinding);
    }

    @Override
    public Set<Expr> getArrayInputs(List<Expr> args)
    {
      return ImmutableSet.copyOf(args);
    }

    @Override
    public void validateArguments(LambdaExpr lambdaExpr, List<Expr> args)
    {
      Preconditions.checkArgument(args.size() > 0);
      if (lambdaExpr.identifierCount() > 0) {
        Preconditions.checkArgument(
            args.size() == lambdaExpr.identifierCount(),
            StringUtils.format("lambda expression argument count does not match %s argument count", name())
        );
      }
    }
  }

  /**
   * Base class for family of {@link ApplyFunction} which aggregate a scalar or array value given one or more array
   * input {@link Expr} arguments and an array or scalar "accumulator" argument with an initial value
   */
  abstract class BaseFoldFunction implements ApplyFunction
  {
    /**
     * Accumulate a value by evaluating a {@link LambdaExpr} for each index position of an
     * {@link IndexableFoldLambdaBinding}
     */
    ExprEval applyFold(LambdaExpr lambdaExpr, Object accumulator, IndexableFoldLambdaBinding bindings)
    {
      for (int i = 0; i < bindings.getLength(); i++) {
        ExprEval evaluated = lambdaExpr.eval(bindings.accumulateWithIndex(i, accumulator));
        accumulator = evaluated.value();
      }
      if (accumulator instanceof Boolean) {
        return ExprEval.of((boolean) accumulator, ExprType.LONG);
      }
      return ExprEval.bestEffortOf(accumulator);
    }

    @Override
    public boolean hasArrayOutput(LambdaExpr lambdaExpr)
    {
      Expr.BindingDetails lambdaBindingDetails = lambdaExpr.analyzeInputs();
      return lambdaBindingDetails.isOutputArray();
    }
  }

  /**
   * Accumulate a value for a single array input with a 2 argument {@link LambdaExpr}. The 'array' input expression is
   * the first argument, the initial value for the accumulator expression is the 2nd argument.
   */
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
      Expr arrayExpr = argsExpr.get(0);
      Expr accExpr = argsExpr.get(1);

      ExprEval arrayEval = arrayExpr.eval(bindings);
      ExprEval accEval = accExpr.eval(bindings);

      Object[] array = arrayEval.asArray();
      if (array == null) {
        return ExprEval.of(null);
      }
      Object accumulator = accEval.value();

      FoldLambdaBinding lambdaBinding = new FoldLambdaBinding(array, accumulator, lambdaExpr, bindings);
      return applyFold(lambdaExpr, accumulator, lambdaBinding);
    }

    @Override
    public Set<Expr> getArrayInputs(List<Expr> args)
    {
      // accumulator argument cannot currently be inferred, so ignore it until we think of something better to do
      return ImmutableSet.of(args.get(0));
    }

    @Override
    public void validateArguments(LambdaExpr lambdaExpr, List<Expr> args)
    {
      Preconditions.checkArgument(args.size() == 2);
      Preconditions.checkArgument(
          args.size() == lambdaExpr.identifierCount(),
          StringUtils.format("lambda expression argument count does not match %s argument count", name())
      );
    }
  }

  /**
   * Accumulate a value for the cartesian product of 'n' array inputs arguments with an 'n + 1' argument
   * {@link LambdaExpr}. The 'array' input expressions are the first 'n' arguments, the initial value for the
   * accumulator expression is the final argument.
   */
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

      Object accumulator = accEval.value();

      CartesianFoldLambdaBinding lambdaBindings =
          new CartesianFoldLambdaBinding(product, accumulator, lambdaExpr, bindings);
      return applyFold(lambdaExpr, accumulator, lambdaBindings);
    }

    @Override
    public Set<Expr> getArrayInputs(List<Expr> args)
    {
      // accumulator argument cannot be inferred, so ignore it until we think of something better to do
      return ImmutableSet.copyOf(args.subList(0, args.size() - 1));
    }

    @Override
    public void validateArguments(LambdaExpr lambdaExpr, List<Expr> args)
    {
      Preconditions.checkArgument(
          args.size() == lambdaExpr.identifierCount(),
          StringUtils.format("lambda expression argument count does not match %s argument count", name())
      );
    }
  }

  /**
   * Filter an array to all elements that evaluate to a 'truthy' value for a {@link LambdaExpr}
   */
  class FilterFunction implements ApplyFunction
  {
    static final String NAME = "filter";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public boolean hasArrayOutput(LambdaExpr lambdaExpr)
    {
      return true;
    }

    @Override
    public ExprEval apply(LambdaExpr lambdaExpr, List<Expr> argsExpr, Expr.ObjectBinding bindings)
    {
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

    @Override
    public void validateArguments(LambdaExpr lambdaExpr, List<Expr> args)
    {
      Preconditions.checkArgument(args.size() == 1);
      Preconditions.checkArgument(
          args.size() == lambdaExpr.identifierCount(),
          StringUtils.format("lambda expression argument count does not match %s argument count", name())
      );
    }

    private <T> Stream<T> filter(T[] array, LambdaExpr expr, SettableLambdaBinding binding)
    {
      return Arrays.stream(array).filter(s -> expr.eval(binding.withBinding(expr.getIdentifier(), s)).asBoolean());
    }
  }

  /**
   * Base class for family of {@link ApplyFunction} which evaluate elements elements of a single array input against
   * a {@link LambdaExpr} to evaluate to a final 'truthy' value
   */
  abstract class MatchFunction implements ApplyFunction
  {
    @Override
    public ExprEval apply(LambdaExpr lambdaExpr, List<Expr> argsExpr, Expr.ObjectBinding bindings)
    {
      Expr arrayExpr = argsExpr.get(0);
      ExprEval arrayEval = arrayExpr.eval(bindings);

      final Object[] array = arrayEval.asArray();
      if (array == null) {
        return ExprEval.of(false, ExprType.LONG);
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

    @Override
    public void validateArguments(LambdaExpr lambdaExpr, List<Expr> args)
    {
      Preconditions.checkArgument(args.size() == 1);
      Preconditions.checkArgument(
          args.size() == lambdaExpr.identifierCount(),
          StringUtils.format("lambda expression argument count does not match %s argument count", name())
      );
    }

    public abstract ExprEval match(Object[] values, LambdaExpr expr, SettableLambdaBinding bindings);
  }

  /**
   * Evaluates to true if any element of the array input {@link Expr} causes the {@link LambdaExpr} to evaluate to a
   * 'truthy' value
   */
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
      return ExprEval.of(anyMatch, ExprType.LONG);
    }
  }

  /**
   * Evaluates to true if all element of the array input {@link Expr} causes the {@link LambdaExpr} to evaluate to a
   * 'truthy' value
   */
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
      return ExprEval.of(allMatch, ExprType.LONG);
    }
  }

  /**
   * Simple, mutable, {@link Expr.ObjectBinding} for a {@link LambdaExpr} which provides a {@link Map} for storing
   * arbitrary values to use as values for {@link IdentifierExpr} in the body of the lambda that are arguments to the
   * lambda
   */
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
      this.bindings = bindings != null ? bindings : Collections.emptyMap()::get;
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

  /**
   * {@link Expr.ObjectBinding} which can be iterated by an integer index position for {@link BaseMapFunction}.
   * Evaluating an {@link IdentifierExpr} against these bindings will return the value(s) of the array at the current
   * index for any lambda identifiers, and fall through to the base {@link Expr.ObjectBinding} for all bindings provided
   * by an outer scope.
   */
  interface IndexableMapLambdaObjectBinding extends Expr.ObjectBinding
  {
    /**
     * Total number of bindings in this binding
     */
    int getLength();

    /**
     * Update index position
     */
    IndexableMapLambdaObjectBinding withIndex(int index);
  }

  /**
   * {@link IndexableMapLambdaObjectBinding} for a {@link MapFunction}. Lambda argument binding is stored in an object
   * array, retrieving binding values for the lambda identifier returns the value at the current index.
   */
  class MapLambdaBinding implements IndexableMapLambdaObjectBinding
  {
    private final Expr.ObjectBinding bindings;
    @Nullable
    private final String lambdaIdentifier;
    private final Object[] arrayValues;
    private int index = 0;
    private final boolean scoped;

    MapLambdaBinding(Object[] arrayValues, LambdaExpr expr, Expr.ObjectBinding bindings)
    {
      this.lambdaIdentifier = expr.getIdentifier();
      this.arrayValues = arrayValues;
      this.bindings = bindings != null ? bindings : Collections.emptyMap()::get;
      this.scoped = lambdaIdentifier != null;
    }

    @Nullable
    @Override
    public Object get(String name)
    {
      if (scoped && name.equals(lambdaIdentifier)) {
        return arrayValues[index];
      }
      return bindings.get(name);
    }

    @Override
    public int getLength()
    {
      return arrayValues.length;
    }

    @Override
    public MapLambdaBinding withIndex(int index)
    {
      this.index = index;
      return this;
    }
  }

  /**
   * {@link IndexableMapLambdaObjectBinding} for a {@link CartesianMapFunction}. Lambda argument bindings stored as a
   * cartesian product in the form of a list of lists of objects, where the inner list is the in order list of values
   * for each {@link LambdaExpr} argument
   */
  class CartesianMapLambdaBinding implements IndexableMapLambdaObjectBinding
  {
    private final Expr.ObjectBinding bindings;
    private final Object2IntMap<String> lambdaIdentifiers;
    private final List<List<Object>> lambdaInputs;
    private final boolean scoped;
    private int index = 0;

    CartesianMapLambdaBinding(List<List<Object>> inputs, LambdaExpr expr, Expr.ObjectBinding bindings)
    {
      this.lambdaInputs = inputs;
      List<String> ids = expr.getIdentifiers();
      this.scoped = ids.size() > 0;
      this.lambdaIdentifiers = new Object2IntArrayMap<>(ids.size());
      for (int i = 0; i < ids.size(); i++) {
        lambdaIdentifiers.put(ids.get(i), i);
      }

      this.bindings = bindings != null ? bindings : Collections.emptyMap()::get;
    }

    @Nullable
    @Override
    public Object get(String name)
    {
      if (scoped && lambdaIdentifiers.containsKey(name)) {
        return lambdaInputs.get(index).get(lambdaIdentifiers.getInt(name));
      }
      return bindings.get(name);
    }

    @Override
    public int getLength()
    {
      return lambdaInputs.size();
    }

    @Override
    public CartesianMapLambdaBinding withIndex(int index)
    {
      this.index = index;
      return this;
    }
  }

  /**
   * {@link Expr.ObjectBinding} which can be iterated by an integer index position for {@link BaseFoldFunction}.
   * Evaluating an {@link IdentifierExpr} against these bindings will return the value(s) of the array at the current
   * index for any lambda array identifiers, the value of the 'accumulator' for the lambda accumulator identifier,
   * and fall through to the base {@link Expr.ObjectBinding} for all bindings provided by an outer scope.
   */
  interface IndexableFoldLambdaBinding extends Expr.ObjectBinding
  {
    /**
     * Total number of bindings in this binding
     */
    int getLength();

    /**
     * Update the index and accumulator value
     */
    IndexableFoldLambdaBinding accumulateWithIndex(int index, Object accumulator);
  }

  /**
   * {@link IndexableFoldLambdaBinding} for a {@link FoldFunction}. Like {@link MapLambdaBinding}
   * but with additional information to track and provide binding values for an accumulator.
   */
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
      this.bindings = bindings != null ? bindings : Collections.emptyMap()::get;
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
    public int getLength()
    {
      return arrayValues.length;
    }

    @Override
    public FoldLambdaBinding accumulateWithIndex(int index, Object acc)
    {
      this.index = index;
      this.accumulatorValue = acc;
      return this;
    }
  }

  /**
   * {@link IndexableFoldLambdaBinding} for a {@link CartesianFoldFunction}. Like {@link CartesianMapLambdaBinding}
   * but with additional information to track and provide binding values for an accumulator.
   */
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
      this.bindings = bindings != null ? bindings : Collections.emptyMap()::get;
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
    public int getLength()
    {
      return lambdaInputs.size();
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
