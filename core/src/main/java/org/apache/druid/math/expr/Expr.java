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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.druid.annotations.SubclassesMustOverrideEqualsAndHashCode;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.math.expr.vector.ExprVectorProcessor;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Base interface of Druid expression language abstract syntax tree nodes. All {@link Expr} implementations are
 * immutable.
 */
@SubclassesMustOverrideEqualsAndHashCode
public interface Expr
{
  String NULL_LITERAL = "null";
  Joiner ARG_JOINER = Joiner.on(", ");

  /**
   * Indicates expression is a constant whose literal value can be extracted by {@link Expr#getLiteralValue()},
   * making evaluating with arguments and bindings unecessary
   */
  default boolean isLiteral()
  {
    // Overridden by things that are literals.
    return false;
  }

  default boolean isNullLiteral()
  {
    // Overridden by things that are null literals.
    return false;
  }

  /**
   * Returns the value of expr if expr is a literal, or throws an exception otherwise.
   *
   * @return {@link ConstantExpr}'s literal value
   *
   * @throws IllegalStateException if expr is not a literal
   */
  @Nullable
  default Object getLiteralValue()
  {
    // Overridden by things that are literals.
    throw new ISE("Not a literal");
  }

  /**
   * Returns an {@link IdentifierExpr} if it is one, else null
   */
  @Nullable
  default IdentifierExpr getIdentifierExprIfIdentifierExpr()
  {
    return null;
  }

  /**
   * Returns the string identifier of an {@link IdentifierExpr}, else null. Use this method to analyze an {@link Expr}
   * tree when trying to distinguish between different {@link IdentifierExpr} with the same
   * {@link IdentifierExpr#binding}. Do NOT use this method to analyze the input binding (e.g. backing column name),
   * use {@link #getBindingIfIdentifier} instead.
   */
  @Nullable
  default String getIdentifierIfIdentifier()
  {
    // overridden by things that are identifiers
    return null;
  }

  /**
   * Returns the string key to use to get a value from {@link Expr.ObjectBinding} of an {@link IdentifierExpr},
   * else null. Use this method to analyze the inputs required to an {@link Expr} tree (e.g. backing column name).
   */
  @Nullable
  default String getBindingIfIdentifier()
  {
    // overridden by things that are identifiers
    return null;
  }

  /**
   * Evaluate the {@link Expr} with the bindings which supply {@link IdentifierExpr} with their values, producing an
   * {@link ExprEval} with the result.
   */
  ExprEval eval(ObjectBinding bindings);

  /**
   * Convert the {@link Expr} back into parseable string that when parsed with
   * {@link Parser#parse(String, ExprMacroTable)} will produce an equivalent {@link Expr}.
   */
  String stringify();

  /**
   * Programatically rewrite the {@link Expr} tree with a {@link Shuttle}. Each {@link Expr} is responsible for
   * ensuring the {@link Shuttle} can visit all of its {@link Expr} children, as well as updating its children
   * {@link Expr} with the results from the {@link Shuttle}, before finally visiting an updated form of itself.
   */
  Expr visit(Shuttle shuttle);


  /**
   * Examine the usage of {@link IdentifierExpr} children of an {@link Expr}, constructing a {@link BindingAnalysis}
   */
  BindingAnalysis analyzeInputs();

  /**
   * Given an {@link InputBindingTypes}, compute what the output {@link ExprType} will be for this expression. A return
   * value of null indicates that the given type information was not enough to resolve the output type, so the
   * expression must be evaluated using default {@link #eval} handling where types are only known after evaluation,
   * through {@link ExprEval#type}.
   */
  @Nullable
  default ExprType getOutputType(InputBindingTypes inputTypes)
  {
    return null;
  }

  /**
   * Check if an expression can be 'vectorized', for a given set of inputs. If this method returns true,
   * {@link #buildVectorized} is expected to produce a {@link ExprVectorProcessor} which can evaluate values in batches
   * to use with vectorized query engines.
   */
  default boolean canVectorize(InputBindingTypes inputTypes)
  {
    return false;
  }

  /**
   * Builds a 'vectorized' expression processor, that can operate on batches of input values for use in vectorized
   * query engines.
   */
  default <T> ExprVectorProcessor<T> buildVectorized(VectorInputBindingTypes inputTypes)
  {
    throw Exprs.cannotVectorize(this);
  }

  /**
   * Mechanism to supply input types for the bindings which will back {@link IdentifierExpr}, to use in the aid of
   * inferring the output type of an expression with {@link #getOutputType}. A null value means that either the binding
   * doesn't exist, or, that the type information is unavailable.
   */
  interface InputBindingTypes
  {
    @Nullable
    ExprType getType(String name);

    /**
     * Check if all provided {@link Expr} can infer the output type as {@link ExprType#isNumeric} with a value of true.
     *
     * There must be at least one expression with a computable numeric output type for this method to return true.
     */
    default boolean areNumeric(List<Expr> args)
    {
      boolean numeric = args.size() > 0;
      for (Expr arg : args) {
        ExprType argType = arg.getOutputType(this);
        if (argType == null) {
          numeric = false;
          break;
        }
        numeric &= argType.isNumeric();
      }
      return numeric;
    }

    /**
     * Check if all provided {@link Expr} can infer the output type as {@link ExprType#isNumeric} with a value of true.
     *
     * There must be at least one expression with a computable numeric output type for this method to return true.
     */
    default boolean areNumeric(Expr... args)
    {
      return areNumeric(Arrays.asList(args));
    }

    /**
     * Check if every provided {@link Expr} computes {@link Expr#canVectorize(InputBindingTypes)} to a value of true
     */
    default boolean canVectorize(List<Expr> args)
    {
      boolean canVectorize = true;
      for (Expr arg : args) {
        canVectorize &= arg.canVectorize(this);
      }
      return canVectorize;
    }

    /**
     * Check if every provided {@link Expr} computes {@link Expr#canVectorize(InputBindingTypes)} to a value of true
     */
    default boolean canVectorize(Expr... args)
    {
      return canVectorize(Arrays.asList(args));
    }
  }

  /**
   * {@link InputBindingTypes} + vectorizations stuff for {@link #buildVectorized}
   */
  interface VectorInputBindingTypes extends InputBindingTypes
  {
    int getMaxVectorSize();
  }

  /**
   * Mechanism to supply values to back {@link IdentifierExpr} during expression evaluation
   */
  interface ObjectBinding
  {
    /**
     * Get value binding for string identifier of {@link IdentifierExpr}
     */
    @Nullable
    Object get(String name);
  }

  /**
   * Mechanism to supply batches of input values to a {@link ExprVectorProcessor} for optimized processing. Mirrors
   * the vectorized column selector interfaces, and includes {@link ExprType} information about all input bindings
   * which exist
   */
  interface VectorInputBinding extends VectorInputBindingTypes
  {
    <T> T[] getObjectVector(String name);

    long[] getLongVector(String name);

    double[] getDoubleVector(String name);

    @Nullable
    boolean[] getNullVector(String name);

    int getCurrentVectorSize();
  }

  /**
   * Mechanism to rewrite an {@link Expr}, implementing a {@link Shuttle} allows visiting all children of an
   * {@link Expr}, and replacing them as desired.
   */
  interface Shuttle
  {
    /**
     * Provide the {@link Shuttle} with an {@link Expr} to inspect and potentially rewrite.
     */
    Expr visit(Expr expr);
  }

  /**
   * Information about the context in which {@link IdentifierExpr} are used in a greater {@link Expr}, listing
   * the 'free variables' (total set of required input columns or values) and distinguishing between which identifiers
   * are used as scalar inputs and which are used as array inputs.
   *
   * This type is primarily used at query time when creating expression column selectors to decide if an expression
   * can properly deal with a multi-valued column input, and also to determine if certain optimizations can be taken.
   *
   * Current implementations of {@link #analyzeInputs()} provide context about {@link Function} and
   * {@link ApplyFunction} arguments which are direct children {@link IdentifierExpr} as scalar or array typed.
   * This is defined by {@link Function#getScalarInputs(List)}, {@link Function#getArrayInputs(List)} and
   * {@link ApplyFunction#getArrayInputs(List)}. Identifiers that are nested inside of argument expressions which
   * are other expression types will not be considered to belong directly to that function, and so are classified by the
   * context their children are using them as instead.
   *
   * This means in rare cases and mostly for "questionable" expressions which we still allow to function 'correctly',
   * these lists might not be fully reliable without a complete type inference system in place. Due to this shortcoming,
   * boolean values {@link BindingAnalysis#hasInputArrays()} and {@link BindingAnalysis#isOutputArray()} are provided to
   * allow functions to explicitly declare that they utilize array typed values, used when determining if some types of
   * optimizations can be applied when constructing the expression column value selector.
   *
   * @see Function#getScalarInputs
   * @see Function#getArrayInputs
   * @see ApplyFunction#getArrayInputs
   * @see Parser#applyUnappliedBindings
   * @see Parser#applyUnapplied
   * @see Parser#liftApplyLambda
   * @see org.apache.druid.segment.virtual.ExpressionSelectors#makeDimensionSelector
   * @see org.apache.druid.segment.virtual.ExpressionSelectors#makeColumnValueSelector
   */
  @SuppressWarnings("JavadocReference")
  class BindingAnalysis
  {
    private final ImmutableSet<IdentifierExpr> freeVariables;
    private final ImmutableSet<IdentifierExpr> scalarVariables;
    private final ImmutableSet<IdentifierExpr> arrayVariables;
    private final boolean hasInputArrays;
    private final boolean isOutputArray;

    BindingAnalysis()
    {
      this(ImmutableSet.of(), ImmutableSet.of(), ImmutableSet.of(), false, false);
    }

    BindingAnalysis(IdentifierExpr expr)
    {
      this(ImmutableSet.of(expr), ImmutableSet.of(), ImmutableSet.of(), false, false);
    }

    private BindingAnalysis(
        ImmutableSet<IdentifierExpr> freeVariables,
        ImmutableSet<IdentifierExpr> scalarVariables,
        ImmutableSet<IdentifierExpr> arrayVariables,
        boolean hasInputArrays,
        boolean isOutputArray
    )
    {
      this.freeVariables = freeVariables;
      this.scalarVariables = scalarVariables;
      this.arrayVariables = arrayVariables;
      this.hasInputArrays = hasInputArrays;
      this.isOutputArray = isOutputArray;
    }

    /**
     * Get the list of required column inputs to evaluate an expression ({@link IdentifierExpr#binding})
     */
    public List<String> getRequiredBindingsList()
    {
      return new ArrayList<>(getRequiredBindings());
    }

    /**
     * Get the set of required column inputs to evaluate an expression ({@link IdentifierExpr#binding})
     */
    public Set<String> getRequiredBindings()
    {
      return map(freeVariables, IdentifierExpr::getBindingIfIdentifier);
    }

    /**
     * Set of {@link IdentifierExpr#binding} which are used as scalar inputs to operators and functions.
     */
    Set<String> getScalarBindings()
    {
      return map(scalarVariables, IdentifierExpr::getBindingIfIdentifier);
    }

    /**
     * Set of {@link IdentifierExpr#binding} which are used as array inputs to operators, functions, and apply
     * functions.
     */
    public Set<String> getArrayBindings()
    {
      return map(arrayVariables, IdentifierExpr::getBindingIfIdentifier);
    }

    /**
     * Total set of 'free' inputs of an {@link Expr}, that are not supplied by a {@link LambdaExpr} binding
     */
    public Set<IdentifierExpr> getFreeVariables()
    {
      return freeVariables;
    }

    /**
     * Set of {@link IdentifierExpr#identifier} which are used as scalar inputs to operators and functions.
     */
    Set<String> getScalarVariables()
    {
      return map(scalarVariables, IdentifierExpr::getIdentifier);
    }

    /**
     * Set of {@link IdentifierExpr#identifier} which are used as array inputs to operators, functions, and apply
     * functions.
     */
    Set<String> getArrayVariables()
    {
      return map(arrayVariables, IdentifierExpr::getIdentifier);
    }

    /**
     * Returns true if any expression in the expression tree has any array inputs. Note that in some cases, this can be
     * true and {@link #getArrayBindings()} or {@link #getArrayVariables()} can be empty.
     *
     * This is because these collections contain identifiers/bindings which were classified as either scalar or array
     * inputs based on the context of their usage by {@link Expr#analyzeInputs()}, where as this value and
     * {@link #isOutputArray()} are set based on information reported by {@link Function#hasArrayInputs()},
     * {@link Function#hasArrayOutput()}, and {@link ApplyFunction#hasArrayOutput(LambdaExpr)}, without regards to
     * identifiers or anything else.
     */
    public boolean hasInputArrays()
    {
      return hasInputArrays;
    }

    /**
     * Returns true if any expression in this expression tree produces array outputs as reported by
     * {@link Function#hasArrayOutput()} or {@link ApplyFunction#hasArrayOutput(LambdaExpr)}
     */
    public boolean isOutputArray()
    {
      return isOutputArray;
    }

    /**
     * Combine with {@link BindingAnalysis} from {@link Expr#analyzeInputs()}
     */
    public BindingAnalysis with(Expr other)
    {
      return with(other.analyzeInputs());
    }

    /**
     * Combine (union) another {@link BindingAnalysis}
     */
    public BindingAnalysis with(BindingAnalysis other)
    {
      return new BindingAnalysis(
          ImmutableSet.copyOf(Sets.union(freeVariables, other.freeVariables)),
          ImmutableSet.copyOf(Sets.union(scalarVariables, other.scalarVariables)),
          ImmutableSet.copyOf(Sets.union(arrayVariables, other.arrayVariables)),
          hasInputArrays || other.hasInputArrays,
          isOutputArray || other.isOutputArray
      );
    }

    /**
     * Add set of arguments as {@link BindingAnalysis#scalarVariables} that are *directly* {@link IdentifierExpr},
     * else they are ignored.
     */
    public BindingAnalysis withScalarArguments(Set<Expr> scalarArguments)
    {
      Set<IdentifierExpr> moreScalars = new HashSet<>();
      for (Expr expr : scalarArguments) {
        final boolean isIdentiferExpr = expr.getIdentifierExprIfIdentifierExpr() != null;
        if (isIdentiferExpr) {
          moreScalars.add((IdentifierExpr) expr);
        }
      }
      return new BindingAnalysis(
          ImmutableSet.copyOf(Sets.union(freeVariables, moreScalars)),
          ImmutableSet.copyOf(Sets.union(scalarVariables, moreScalars)),
          arrayVariables,
          hasInputArrays,
          isOutputArray
      );
    }

    /**
     * Add set of arguments as {@link BindingAnalysis#arrayVariables} that are *directly* {@link IdentifierExpr},
     * else they are ignored.
     */
    BindingAnalysis withArrayArguments(Set<Expr> arrayArguments)
    {
      Set<IdentifierExpr> arrayIdentifiers = new HashSet<>();
      for (Expr expr : arrayArguments) {
        final boolean isIdentifierExpr = expr.getIdentifierExprIfIdentifierExpr() != null;
        if (isIdentifierExpr) {
          arrayIdentifiers.add((IdentifierExpr) expr);
        }
      }
      return new BindingAnalysis(
          ImmutableSet.copyOf(Sets.union(freeVariables, arrayIdentifiers)),
          scalarVariables,
          ImmutableSet.copyOf(Sets.union(arrayVariables, arrayIdentifiers)),
          hasInputArrays || !arrayArguments.isEmpty(),
          isOutputArray
      );
    }

    /**
     * Copy, setting if an expression has array inputs
     */
    BindingAnalysis withArrayInputs(boolean hasArrays)
    {
      return new BindingAnalysis(
          freeVariables,
          scalarVariables,
          arrayVariables,
          hasArrays || !arrayVariables.isEmpty(),
          isOutputArray
      );
    }

    /**
     * Copy, setting if an expression produces an array output
     */
    BindingAnalysis withArrayOutput(boolean isOutputArray)
    {
      return new BindingAnalysis(
          freeVariables,
          scalarVariables,
          arrayVariables,
          hasInputArrays,
          isOutputArray
      );
    }

    /**
     * Remove any {@link IdentifierExpr} that are from a {@link LambdaExpr}, since the {@link ApplyFunction} will
     * provide bindings for these variables.
     */
    BindingAnalysis removeLambdaArguments(Set<String> lambda)
    {
      return new BindingAnalysis(
          ImmutableSet.copyOf(freeVariables.stream().filter(x -> !lambda.contains(x.getIdentifier())).iterator()),
          ImmutableSet.copyOf(scalarVariables.stream().filter(x -> !lambda.contains(x.getIdentifier())).iterator()),
          ImmutableSet.copyOf(arrayVariables.stream().filter(x -> !lambda.contains(x.getIdentifier())).iterator()),
          hasInputArrays,
          isOutputArray
      );
    }

    // Use this instead of streams for better performance
    private static Set<String> map(
        Set<IdentifierExpr> variables,
        java.util.function.Function<IdentifierExpr, String> mapper
    )
    {
      Set<String> results = Sets.newHashSetWithExpectedSize(variables.size());
      for (IdentifierExpr variable : variables) {
        results.add(mapper.apply(variable));
      }
      return results;
    }
  }
}
