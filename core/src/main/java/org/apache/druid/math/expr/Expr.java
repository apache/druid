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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.druid.annotations.SubclassesMustOverrideEqualsAndHashCode;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Comparators;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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
   * Programmatically inspect the {@link Expr} tree with a {@link Visitor}. Each {@link Expr} is responsible for
   * ensuring the {@link Visitor} can visit all of its {@link Expr} children before visiting itself
   */
  void visit(Visitor visitor);

  /**
   * Programatically rewrite the {@link Expr} tree with a {@link Shuttle}.Each {@link Expr} is responsible for
   * ensuring the {@link Shuttle} can visit all of its {@link Expr} children, as well as updating its children
   * {@link Expr} with the results from the {@link Shuttle}, before finally visiting an updated form of itself.
   */
  Expr visit(Shuttle shuttle);

  /**
   * Examine the usage of {@link IdentifierExpr} children of an {@link Expr}, constructing a {@link BindingDetails}
   */
  BindingDetails analyzeInputs();

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
   * Mechanism to inspect an {@link Expr}, implementing a {@link Visitor} allows visiting all children of an
   * {@link Expr}
   */
  interface Visitor
  {
    /**
     * Provide the {@link Visitor} with an {@link Expr} to inspect
     */
    void visit(Expr expr);
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
   * boolean values {@link BindingDetails#hasInputArrays()} and {@link BindingDetails#isOutputArray()} are provided to
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
  class BindingDetails
  {
    private final ImmutableSet<IdentifierExpr> freeVariables;
    private final ImmutableSet<IdentifierExpr> scalarVariables;
    private final ImmutableSet<IdentifierExpr> arrayVariables;
    private final boolean hasInputArrays;
    private final boolean isOutputArray;

    BindingDetails()
    {
      this(ImmutableSet.of(), ImmutableSet.of(), ImmutableSet.of(), false, false);
    }

    BindingDetails(IdentifierExpr expr)
    {
      this(ImmutableSet.of(expr), ImmutableSet.of(), ImmutableSet.of(), false, false);
    }

    private BindingDetails(
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
     * Combine with {@link BindingDetails} from {@link Expr#analyzeInputs()}
     */
    public BindingDetails with(Expr other)
    {
      return with(other.analyzeInputs());
    }

    /**
     * Combine (union) another {@link BindingDetails}
     */
    public BindingDetails with(BindingDetails other)
    {
      return new BindingDetails(
          ImmutableSet.copyOf(Sets.union(freeVariables, other.freeVariables)),
          ImmutableSet.copyOf(Sets.union(scalarVariables, other.scalarVariables)),
          ImmutableSet.copyOf(Sets.union(arrayVariables, other.arrayVariables)),
          hasInputArrays || other.hasInputArrays,
          isOutputArray || other.isOutputArray
      );
    }

    /**
     * Add set of arguments as {@link BindingDetails#scalarVariables} that are *directly* {@link IdentifierExpr},
     * else they are ignored.
     */
    public BindingDetails withScalarArguments(Set<Expr> scalarArguments)
    {
      Set<IdentifierExpr> moreScalars = new HashSet<>();
      for (Expr expr : scalarArguments) {
        final boolean isIdentiferExpr = expr.getIdentifierExprIfIdentifierExpr() != null;
        if (isIdentiferExpr) {
          moreScalars.add((IdentifierExpr) expr);
        }
      }
      return new BindingDetails(
          ImmutableSet.copyOf(Sets.union(freeVariables, moreScalars)),
          ImmutableSet.copyOf(Sets.union(scalarVariables, moreScalars)),
          arrayVariables,
          hasInputArrays,
          isOutputArray
      );
    }

    /**
     * Add set of arguments as {@link BindingDetails#arrayVariables} that are *directly* {@link IdentifierExpr},
     * else they are ignored.
     */
    BindingDetails withArrayArguments(Set<Expr> arrayArguments)
    {
      Set<IdentifierExpr> arrayIdentifiers = new HashSet<>();
      for (Expr expr : arrayArguments) {
        final boolean isIdentifierExpr = expr.getIdentifierExprIfIdentifierExpr() != null;
        if (isIdentifierExpr) {
          arrayIdentifiers.add((IdentifierExpr) expr);
        }
      }
      return new BindingDetails(
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
    BindingDetails withArrayInputs(boolean hasArrays)
    {
      return new BindingDetails(
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
    BindingDetails withArrayOutput(boolean isOutputArray)
    {
      return new BindingDetails(
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
    BindingDetails removeLambdaArguments(Set<String> lambda)
    {
      return new BindingDetails(
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

/**
 * Base type for all constant expressions. {@link ConstantExpr} allow for direct value extraction without evaluating
 * {@link Expr.ObjectBinding}. {@link ConstantExpr} are terminal nodes of an expression tree, and have no children
 * {@link Expr}.
 */
abstract class ConstantExpr implements Expr
{
  @Override
  public boolean isLiteral()
  {
    return true;
  }

  @Override
  public void visit(Visitor visitor)
  {
    visitor.visit(this);
  }

  @Override
  public Expr visit(Shuttle shuttle)
  {
    return shuttle.visit(this);
  }

  @Override
  public BindingDetails analyzeInputs()
  {
    return new BindingDetails();
  }

  @Override
  public String stringify()
  {
    return toString();
  }
}

abstract class NullNumericConstantExpr extends ConstantExpr
{
  @Override
  public Object getLiteralValue()
  {
    return null;
  }

  @Override
  public String toString()
  {
    return NULL_LITERAL;
  }
}

class LongExpr extends ConstantExpr
{
  private final Long value;

  LongExpr(Long value)
  {
    this.value = Preconditions.checkNotNull(value, "value");
  }

  @Override
  public Object getLiteralValue()
  {
    return value;
  }

  @Override
  public String toString()
  {
    return String.valueOf(value);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return ExprEval.ofLong(value);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LongExpr longExpr = (LongExpr) o;
    return Objects.equals(value, longExpr.value);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(value);
  }
}

class NullLongExpr extends NullNumericConstantExpr
{
  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return ExprEval.ofLong(null);
  }

  @Override
  public final int hashCode()
  {
    return NullLongExpr.class.hashCode();
  }

  @Override
  public final boolean equals(Object obj)
  {
    return obj instanceof NullLongExpr;
  }
}


class LongArrayExpr extends ConstantExpr
{
  private final Long[] value;

  LongArrayExpr(Long[] value)
  {
    this.value = Preconditions.checkNotNull(value, "value");
  }

  @Override
  public Object getLiteralValue()
  {
    return value;
  }

  @Override
  public String toString()
  {
    return Arrays.toString(value);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return ExprEval.ofLongArray(value);
  }

  @Override
  public String stringify()
  {
    if (value.length == 0) {
      return "<LONG>[]";
    }
    return StringUtils.format("<LONG>%s", toString());
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LongArrayExpr that = (LongArrayExpr) o;
    return Arrays.equals(value, that.value);
  }

  @Override
  public int hashCode()
  {
    return Arrays.hashCode(value);
  }
}

class StringExpr extends ConstantExpr
{
  @Nullable
  private final String value;

  StringExpr(@Nullable String value)
  {
    this.value = NullHandling.emptyToNullIfNeeded(value);
  }

  @Nullable
  @Override
  public Object getLiteralValue()
  {
    return value;
  }

  @Override
  public String toString()
  {
    return value;
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return ExprEval.of(value);
  }

  @Override
  public String stringify()
  {
    // escape as javascript string since string literals are wrapped in single quotes
    return value == null ? NULL_LITERAL : StringUtils.format("'%s'", StringEscapeUtils.escapeJavaScript(value));
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StringExpr that = (StringExpr) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(value);
  }
}

class StringArrayExpr extends ConstantExpr
{
  private final String[] value;

  StringArrayExpr(String[] value)
  {
    this.value = Preconditions.checkNotNull(value, "value");
  }

  @Override
  public Object getLiteralValue()
  {
    return value;
  }

  @Override
  public String toString()
  {
    return Arrays.toString(value);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return ExprEval.ofStringArray(value);
  }

  @Override
  public String stringify()
  {
    if (value.length == 0) {
      return "<STRING>[]";
    }

    return StringUtils.format(
        "<STRING>[%s]",
        ARG_JOINER.join(
            Arrays.stream(value)
                  .map(s -> s == null
                            ? NULL_LITERAL
                            // escape as javascript string since string literals are wrapped in single quotes
                            : StringUtils.format("'%s'", StringEscapeUtils.escapeJavaScript(s))
                  )
                  .iterator()
        )
    );
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StringArrayExpr that = (StringArrayExpr) o;
    return Arrays.equals(value, that.value);
  }

  @Override
  public int hashCode()
  {
    return Arrays.hashCode(value);
  }
}

class DoubleExpr extends ConstantExpr
{
  private final Double value;

  DoubleExpr(Double value)
  {
    this.value = Preconditions.checkNotNull(value, "value");
  }

  @Override
  public Object getLiteralValue()
  {
    return value;
  }

  @Override
  public String toString()
  {
    return String.valueOf(value);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return ExprEval.ofDouble(value);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DoubleExpr that = (DoubleExpr) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(value);
  }
}

class NullDoubleExpr extends NullNumericConstantExpr
{
  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return ExprEval.ofDouble(null);
  }

  @Override
  public final int hashCode()
  {
    return NullDoubleExpr.class.hashCode();
  }

  @Override
  public final boolean equals(Object obj)
  {
    return obj instanceof NullDoubleExpr;
  }
}

class DoubleArrayExpr extends ConstantExpr
{
  private final Double[] value;

  DoubleArrayExpr(Double[] value)
  {
    this.value = Preconditions.checkNotNull(value, "value");
  }

  @Override
  public Object getLiteralValue()
  {
    return value;
  }

  @Override
  public String toString()
  {
    return Arrays.toString(value);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return ExprEval.ofDoubleArray(value);
  }

  @Override
  public String stringify()
  {
    if (value.length == 0) {
      return "<DOUBLE>[]";
    }
    return StringUtils.format("<DOUBLE>%s", toString());
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DoubleArrayExpr that = (DoubleArrayExpr) o;
    return Arrays.equals(value, that.value);
  }

  @Override
  public int hashCode()
  {
    return Arrays.hashCode(value);
  }
}

/**
 * This {@link Expr} node is used to represent a variable in the expression language. At evaluation time, the string
 * identifier will be used to retrieve the runtime value for the variable from {@link Expr.ObjectBinding}.
 * {@link IdentifierExpr} are terminal nodes of an expression tree, and have no children {@link Expr}.
 */
class IdentifierExpr implements Expr
{
  private final String identifier;
  private final String binding;

  /**
   * Construct a identifier expression for a {@link LambdaExpr}, where the {@link #identifier} is equal to
   * {@link #binding}
   */
  IdentifierExpr(String value)
  {
    this.identifier = value;
    this.binding = value;
  }

  /**
   * Construct a normal identifier expression, where {@link #binding} is the key to fetch the backing value from
   * {@link Expr.ObjectBinding} and the {@link #identifier} is a unique string that identifies this usage of the
   * binding.
   */
  IdentifierExpr(String identifier, String binding)
  {
    this.identifier = identifier;
    this.binding = binding;
  }

  @Override
  public String toString()
  {
    return binding;
  }

  /**
   * Unique identifier for the binding
   */
  @Nullable
  public String getIdentifier()
  {
    return identifier;
  }

  /**
   * Value binding, key to retrieve value from {@link Expr.ObjectBinding#get(String)}
   */
  @Nullable
  public String getBinding()
  {
    return binding;
  }

  @Nullable
  @Override
  public String getIdentifierIfIdentifier()
  {
    return identifier;
  }

  @Nullable
  @Override
  public String getBindingIfIdentifier()
  {
    return binding;
  }

  @Nullable
  @Override
  public IdentifierExpr getIdentifierExprIfIdentifierExpr()
  {
    return this;
  }

  @Override
  public BindingDetails analyzeInputs()
  {
    return new BindingDetails(this);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return ExprEval.bestEffortOf(bindings.get(binding));
  }

  @Override
  public String stringify()
  {
    // escape as java strings since identifiers are wrapped in double quotes
    return StringUtils.format("\"%s\"", StringEscapeUtils.escapeJava(binding));
  }

  @Override
  public void visit(Visitor visitor)
  {
    visitor.visit(this);
  }

  @Override
  public Expr visit(Shuttle shuttle)
  {
    return shuttle.visit(this);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IdentifierExpr that = (IdentifierExpr) o;
    return Objects.equals(identifier, that.identifier);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(identifier);
  }
}

class LambdaExpr implements Expr
{
  private final ImmutableList<IdentifierExpr> args;
  private final Expr expr;

  LambdaExpr(List<IdentifierExpr> args, Expr expr)
  {
    this.args = ImmutableList.copyOf(args);
    this.expr = expr;
  }

  @Override
  public String toString()
  {
    return StringUtils.format("(%s -> %s)", args, expr);
  }

  int identifierCount()
  {
    return args.size();
  }

  @Nullable
  public String getIdentifier()
  {
    Preconditions.checkState(args.size() < 2, "LambdaExpr has multiple arguments");
    if (args.size() == 1) {
      return args.get(0).toString();
    }
    return null;
  }

  public List<String> getIdentifiers()
  {
    return args.stream().map(IdentifierExpr::toString).collect(Collectors.toList());
  }

  ImmutableList<IdentifierExpr> getIdentifierExprs()
  {
    return args;
  }

  public Expr getExpr()
  {
    return expr;
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return expr.eval(bindings);
  }

  @Override
  public String stringify()
  {
    return StringUtils.format("(%s) -> %s", ARG_JOINER.join(getIdentifiers()), expr.stringify());
  }

  @Override
  public void visit(Visitor visitor)
  {
    expr.visit(visitor);
    visitor.visit(this);
  }

  @Override
  public Expr visit(Shuttle shuttle)
  {
    List<IdentifierExpr> newArgs =
        args.stream().map(arg -> (IdentifierExpr) shuttle.visit(arg)).collect(Collectors.toList());
    Expr newBody = expr.visit(shuttle);
    return shuttle.visit(new LambdaExpr(newArgs, newBody));
  }

  @Override
  public BindingDetails analyzeInputs()
  {
    final Set<String> lambdaArgs = args.stream().map(IdentifierExpr::toString).collect(Collectors.toSet());
    BindingDetails bodyDetails = expr.analyzeInputs();
    return bodyDetails.removeLambdaArguments(lambdaArgs);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LambdaExpr that = (LambdaExpr) o;
    return Objects.equals(args, that.args) &&
           Objects.equals(expr, that.expr);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(args, expr);
  }
}

/**
 * {@link Expr} node for a {@link Function} call. {@link FunctionExpr} has children {@link Expr} in the form of the
 * list of arguments that are passed to the {@link Function} along with the {@link Expr.ObjectBinding} when it is
 * evaluated.
 */
class FunctionExpr implements Expr
{
  final Function function;
  final ImmutableList<Expr> args;
  private final String name;

  FunctionExpr(Function function, String name, List<Expr> args)
  {
    this.function = function;
    this.name = name;
    this.args = ImmutableList.copyOf(args);
    function.validateArguments(args);
  }

  @Override
  public String toString()
  {
    return StringUtils.format("(%s %s)", name, args);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return function.apply(args, bindings);
  }

  @Override
  public String stringify()
  {
    return StringUtils.format("%s(%s)", name, ARG_JOINER.join(args.stream().map(Expr::stringify).iterator()));
  }

  @Override
  public void visit(Visitor visitor)
  {
    for (Expr child : args) {
      child.visit(visitor);
    }
    visitor.visit(this);
  }

  @Override
  public Expr visit(Shuttle shuttle)
  {
    List<Expr> newArgs = args.stream().map(shuttle::visit).collect(Collectors.toList());
    return shuttle.visit(new FunctionExpr(function, name, newArgs));
  }

  @Override
  public BindingDetails analyzeInputs()
  {
    BindingDetails accumulator = new BindingDetails();

    for (Expr arg : args) {
      accumulator = accumulator.with(arg);
    }
    return accumulator.withScalarArguments(function.getScalarInputs(args))
                      .withArrayArguments(function.getArrayInputs(args))
                      .withArrayInputs(function.hasArrayInputs())
                      .withArrayOutput(function.hasArrayOutput());
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FunctionExpr that = (FunctionExpr) o;
    return args.equals(that.args) &&
           name.equals(that.name);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(args, name);
  }
}

/**
 * This {@link Expr} node is representative of an {@link ApplyFunction}, and has children in the form of a
 * {@link LambdaExpr} and the list of {@link Expr} arguments that are combined with {@link Expr.ObjectBinding} to
 * evaluate the {@link LambdaExpr}.
 */
class ApplyFunctionExpr implements Expr
{
  final ApplyFunction function;
  final String name;
  final LambdaExpr lambdaExpr;
  final ImmutableList<Expr> argsExpr;
  final BindingDetails bindingDetails;
  final BindingDetails lambdaBindingDetails;
  final ImmutableList<BindingDetails> argsBindingDetails;

  ApplyFunctionExpr(ApplyFunction function, String name, LambdaExpr expr, List<Expr> args)
  {
    this.function = function;
    this.name = name;
    this.argsExpr = ImmutableList.copyOf(args);
    this.lambdaExpr = expr;

    function.validateArguments(expr, args);

    // apply function expressions are examined during expression selector creation, so precompute and cache the
    // binding details of children
    ImmutableList.Builder<BindingDetails> argBindingDetailsBuilder = ImmutableList.builder();
    BindingDetails accumulator = new BindingDetails();
    for (Expr arg : argsExpr) {
      BindingDetails argDetails = arg.analyzeInputs();
      argBindingDetailsBuilder.add(argDetails);
      accumulator = accumulator.with(argDetails);
    }

    lambdaBindingDetails = lambdaExpr.analyzeInputs();

    bindingDetails = accumulator.with(lambdaBindingDetails)
                                .withArrayArguments(function.getArrayInputs(argsExpr))
                                .withArrayInputs(true)
                                .withArrayOutput(function.hasArrayOutput(lambdaExpr));
    argsBindingDetails = argBindingDetailsBuilder.build();
  }

  @Override
  public String toString()
  {
    return StringUtils.format("(%s %s, %s)", name, lambdaExpr, argsExpr);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return function.apply(lambdaExpr, argsExpr, bindings);
  }

  @Override
  public String stringify()
  {
    return StringUtils.format(
        "%s(%s, %s)",
        name,
        lambdaExpr.stringify(),
        ARG_JOINER.join(argsExpr.stream().map(Expr::stringify).iterator())
    );
  }

  @Override
  public void visit(Visitor visitor)
  {
    lambdaExpr.visit(visitor);
    for (Expr arg : argsExpr) {
      arg.visit(visitor);
    }
    visitor.visit(this);
  }

  @Override
  public Expr visit(Shuttle shuttle)
  {
    LambdaExpr newLambda = (LambdaExpr) lambdaExpr.visit(shuttle);
    List<Expr> newArgs = argsExpr.stream().map(shuttle::visit).collect(Collectors.toList());
    return shuttle.visit(new ApplyFunctionExpr(function, name, newLambda, newArgs));
  }

  @Override
  public BindingDetails analyzeInputs()
  {
    return bindingDetails;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ApplyFunctionExpr that = (ApplyFunctionExpr) o;
    return name.equals(that.name) &&
           lambdaExpr.equals(that.lambdaExpr) &&
           argsExpr.equals(that.argsExpr);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, lambdaExpr, argsExpr);
  }
}

/**
 * Base type for all single argument operators, with a single {@link Expr} child for the operand.
 */
abstract class UnaryExpr implements Expr
{
  final Expr expr;

  UnaryExpr(Expr expr)
  {
    this.expr = expr;
  }

  abstract UnaryExpr copy(Expr expr);

  @Override
  public void visit(Visitor visitor)
  {
    expr.visit(visitor);
    visitor.visit(this);
  }

  @Override
  public Expr visit(Shuttle shuttle)
  {
    Expr newExpr = expr.visit(shuttle);
    //noinspection ObjectEquality (checking for object equality here is intentional)
    if (newExpr != expr) {
      return shuttle.visit(copy(newExpr));
    }
    return shuttle.visit(this);
  }

  @Override
  public BindingDetails analyzeInputs()
  {
    // currently all unary operators only operate on scalar inputs
    return expr.analyzeInputs().withScalarArguments(ImmutableSet.of(expr));
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UnaryExpr unaryExpr = (UnaryExpr) o;
    return Objects.equals(expr, unaryExpr.expr);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(expr);
  }
}

class UnaryMinusExpr extends UnaryExpr
{
  UnaryMinusExpr(Expr expr)
  {
    super(expr);
  }

  @Override
  UnaryExpr copy(Expr expr)
  {
    return new UnaryMinusExpr(expr);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    ExprEval ret = expr.eval(bindings);
    if (NullHandling.sqlCompatible() && (ret.value() == null)) {
      return ExprEval.of(null);
    }
    if (ret.type() == ExprType.LONG) {
      return ExprEval.of(-ret.asLong());
    }
    if (ret.type() == ExprType.DOUBLE) {
      return ExprEval.of(-ret.asDouble());
    }
    throw new IAE("unsupported type " + ret.type());
  }

  @Override
  public String stringify()
  {
    return StringUtils.format("-%s", expr.stringify());
  }

  @Override
  public String toString()
  {
    return StringUtils.format("-%s", expr);
  }
}

class UnaryNotExpr extends UnaryExpr
{
  UnaryNotExpr(Expr expr)
  {
    super(expr);
  }

  @Override
  UnaryExpr copy(Expr expr)
  {
    return new UnaryNotExpr(expr);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    ExprEval ret = expr.eval(bindings);
    if (NullHandling.sqlCompatible() && (ret.value() == null)) {
      return ExprEval.of(null);
    }
    // conforming to other boolean-returning binary operators
    ExprType retType = ret.type() == ExprType.DOUBLE ? ExprType.DOUBLE : ExprType.LONG;
    return ExprEval.of(!ret.asBoolean(), retType);
  }

  @Override
  public String stringify()
  {
    return StringUtils.format("!%s", expr.stringify());
  }

  @Override
  public String toString()
  {
    return StringUtils.format("!%s", expr);
  }
}

/**
 * Base type for all binary operators, this {@link Expr} has two children {@link Expr} for the left and right side
 * operands.
 *
 * Note: all concrete subclass of this should have constructor with the form of <init>(String, Expr, Expr)
 * if it's not possible, just be sure Evals.binaryOp() can handle that
 */
abstract class BinaryOpExprBase implements Expr
{
  protected final String op;
  protected final Expr left;
  protected final Expr right;

  BinaryOpExprBase(String op, Expr left, Expr right)
  {
    this.op = op;
    this.left = left;
    this.right = right;
  }

  @Override
  public void visit(Visitor visitor)
  {
    left.visit(visitor);
    right.visit(visitor);
    visitor.visit(this);
  }

  @Override
  public Expr visit(Shuttle shuttle)
  {
    Expr newLeft = left.visit(shuttle);
    Expr newRight = right.visit(shuttle);
    //noinspection ObjectEquality (checking for object equality here is intentional)
    if (left != newLeft || right != newRight) {
      return shuttle.visit(copy(newLeft, newRight));
    }
    return shuttle.visit(this);
  }

  @Override
  public String toString()
  {
    return StringUtils.format("(%s %s %s)", op, left, right);
  }

  @Override
  public String stringify()
  {
    return StringUtils.format("(%s %s %s)", left.stringify(), op, right.stringify());
  }

  protected abstract BinaryOpExprBase copy(Expr left, Expr right);

  @Override
  public BindingDetails analyzeInputs()
  {
    // currently all binary operators operate on scalar inputs
    return left.analyzeInputs().with(right).withScalarArguments(ImmutableSet.of(left, right));
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BinaryOpExprBase that = (BinaryOpExprBase) o;
    return Objects.equals(op, that.op) &&
           Objects.equals(left, that.left) &&
           Objects.equals(right, that.right);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(op, left, right);
  }
}

/**
 * Base class for numerical binary operators, with additional methods defined to evaluate primitive values directly
 * instead of wrapped with {@link ExprEval}
 */
abstract class BinaryEvalOpExprBase extends BinaryOpExprBase
{
  BinaryEvalOpExprBase(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    ExprEval leftVal = left.eval(bindings);
    ExprEval rightVal = right.eval(bindings);

    // Result of any Binary expressions is null if any of the argument is null.
    // e.g "select null * 2 as c;" or "select null + 1 as c;" will return null as per Standard SQL spec.
    if (NullHandling.sqlCompatible() && (leftVal.value() == null || rightVal.value() == null)) {
      return ExprEval.of(null);
    }

    if (leftVal.type() == ExprType.STRING && rightVal.type() == ExprType.STRING) {
      return evalString(leftVal.asString(), rightVal.asString());
    } else if (leftVal.type() == ExprType.LONG && rightVal.type() == ExprType.LONG) {
      if (NullHandling.sqlCompatible() && (leftVal.isNumericNull() || rightVal.isNumericNull())) {
        return ExprEval.of(null);
      }
      return ExprEval.of(evalLong(leftVal.asLong(), rightVal.asLong()));
    } else {
      if (NullHandling.sqlCompatible() && (leftVal.isNumericNull() || rightVal.isNumericNull())) {
        return ExprEval.of(null);
      }
      return ExprEval.of(evalDouble(leftVal.asDouble(), rightVal.asDouble()));
    }
  }

  protected ExprEval evalString(@Nullable String left, @Nullable String right)
  {
    throw new IllegalArgumentException("unsupported type " + ExprType.STRING);
  }

  protected abstract long evalLong(long left, long right);

  protected abstract double evalDouble(double left, double right);
}

class BinMinusExpr extends BinaryEvalOpExprBase
{
  BinMinusExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinMinusExpr(op, left, right);
  }

  @Override
  protected final long evalLong(long left, long right)
  {
    return left - right;
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return left - right;
  }
}

class BinPowExpr extends BinaryEvalOpExprBase
{
  BinPowExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinPowExpr(op, left, right);
  }

  @Override
  protected final long evalLong(long left, long right)
  {
    return LongMath.pow(left, Ints.checkedCast(right));
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return Math.pow(left, right);
  }
}

class BinMulExpr extends BinaryEvalOpExprBase
{
  BinMulExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinMulExpr(op, left, right);
  }

  @Override
  protected final long evalLong(long left, long right)
  {
    return left * right;
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return left * right;
  }
}

class BinDivExpr extends BinaryEvalOpExprBase
{
  BinDivExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinDivExpr(op, left, right);
  }

  @Override
  protected final long evalLong(long left, long right)
  {
    return left / right;
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return left / right;
  }
}

class BinModuloExpr extends BinaryEvalOpExprBase
{
  BinModuloExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinModuloExpr(op, left, right);
  }

  @Override
  protected final long evalLong(long left, long right)
  {
    return left % right;
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return left % right;
  }
}

class BinPlusExpr extends BinaryEvalOpExprBase
{
  BinPlusExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinPlusExpr(op, left, right);
  }

  @Override
  protected ExprEval evalString(@Nullable String left, @Nullable String right)
  {
    return ExprEval.of(NullHandling.nullToEmptyIfNeeded(left)
                       + NullHandling.nullToEmptyIfNeeded(right));
  }

  @Override
  protected final long evalLong(long left, long right)
  {
    return left + right;
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return left + right;
  }
}

class BinLtExpr extends BinaryEvalOpExprBase
{
  BinLtExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinLtExpr(op, left, right);
  }

  @Override
  protected ExprEval evalString(@Nullable String left, @Nullable String right)
  {
    return ExprEval.of(Comparators.<String>naturalNullsFirst().compare(left, right) < 0, ExprType.LONG);
  }

  @Override
  protected final long evalLong(long left, long right)
  {
    return Evals.asLong(left < right);
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    // Use Double.compare for more consistent NaN handling.
    return Evals.asDouble(Double.compare(left, right) < 0);
  }
}

class BinLeqExpr extends BinaryEvalOpExprBase
{
  BinLeqExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinLeqExpr(op, left, right);
  }

  @Override
  protected ExprEval evalString(@Nullable String left, @Nullable String right)
  {
    return ExprEval.of(Comparators.<String>naturalNullsFirst().compare(left, right) <= 0, ExprType.LONG);
  }

  @Override
  protected final long evalLong(long left, long right)
  {
    return Evals.asLong(left <= right);
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    // Use Double.compare for more consistent NaN handling.
    return Evals.asDouble(Double.compare(left, right) <= 0);
  }
}

class BinGtExpr extends BinaryEvalOpExprBase
{
  BinGtExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinGtExpr(op, left, right);
  }

  @Override
  protected ExprEval evalString(@Nullable String left, @Nullable String right)
  {
    return ExprEval.of(Comparators.<String>naturalNullsFirst().compare(left, right) > 0, ExprType.LONG);
  }

  @Override
  protected final long evalLong(long left, long right)
  {
    return Evals.asLong(left > right);
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    // Use Double.compare for more consistent NaN handling.
    return Evals.asDouble(Double.compare(left, right) > 0);
  }
}

class BinGeqExpr extends BinaryEvalOpExprBase
{
  BinGeqExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinGeqExpr(op, left, right);
  }

  @Override
  protected ExprEval evalString(@Nullable String left, @Nullable String right)
  {
    return ExprEval.of(Comparators.<String>naturalNullsFirst().compare(left, right) >= 0, ExprType.LONG);
  }

  @Override
  protected final long evalLong(long left, long right)
  {
    return Evals.asLong(left >= right);
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    // Use Double.compare for more consistent NaN handling.
    return Evals.asDouble(Double.compare(left, right) >= 0);
  }
}

class BinEqExpr extends BinaryEvalOpExprBase
{
  BinEqExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinEqExpr(op, left, right);
  }

  @Override
  protected ExprEval evalString(@Nullable String left, @Nullable String right)
  {
    return ExprEval.of(Objects.equals(left, right), ExprType.LONG);
  }

  @Override
  protected final long evalLong(long left, long right)
  {
    return Evals.asLong(left == right);
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return Evals.asDouble(left == right);
  }
}

class BinNeqExpr extends BinaryEvalOpExprBase
{
  BinNeqExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinNeqExpr(op, left, right);
  }

  @Override
  protected ExprEval evalString(@Nullable String left, @Nullable String right)
  {
    return ExprEval.of(!Objects.equals(left, right), ExprType.LONG);
  }

  @Override
  protected final long evalLong(long left, long right)
  {
    return Evals.asLong(left != right);
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return Evals.asDouble(left != right);
  }
}

class BinAndExpr extends BinaryOpExprBase
{
  BinAndExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinAndExpr(op, left, right);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    ExprEval leftVal = left.eval(bindings);
    return leftVal.asBoolean() ? right.eval(bindings) : leftVal;
  }
}

class BinOrExpr extends BinaryOpExprBase
{
  BinOrExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinOrExpr(op, left, right);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    ExprEval leftVal = left.eval(bindings);
    return leftVal.asBoolean() ? leftVal : right.eval(bindings);
  }

}

