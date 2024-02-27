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
import com.google.common.collect.ImmutableList;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
import org.apache.druid.segment.column.Types;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
final class FunctionalExpr
{
  // phony class to enable maven to track the compilation of this class
}

@SuppressWarnings("ClassName")
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
    Preconditions.checkState(args.size() < 2, "LambdaExpr has multiple arguments, use getIdentifiers");
    if (args.size() == 1) {
      return args.get(0).toString();
    }
    return null;
  }

  public List<String> getIdentifiers()
  {
    return args.stream().map(IdentifierExpr::toString).collect(Collectors.toList());
  }

  public List<String> stringifyIdentifiers()
  {
    return args.stream().map(IdentifierExpr::stringify).collect(Collectors.toList());
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
  public boolean canVectorize(InputBindingInspector inspector)
  {
    return expr.canVectorize(inspector);
  }

  @Override
  public <T> ExprVectorProcessor<T> asVectorProcessor(VectorInputBindingInspector inspector)
  {
    return expr.asVectorProcessor(inspector);
  }

  @Override
  public ExprEval eval(ObjectBinding bindings)
  {
    return expr.eval(bindings);
  }

  @Override
  public String stringify()
  {
    return StringUtils.format("(%s) -> %s", ARG_JOINER.join(stringifyIdentifiers()), expr.stringify());
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
  public BindingAnalysis analyzeInputs()
  {
    final Set<String> lambdaArgs = args.stream().map(IdentifierExpr::toString).collect(Collectors.toSet());
    BindingAnalysis bodyDetails = expr.analyzeInputs();
    return bodyDetails.removeLambdaArguments(lambdaArgs);
  }

  @Override
  public ExpressionType getOutputType(InputBindingInspector inspector)
  {
    return expr.getOutputType(inspector);
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
@SuppressWarnings("ClassName")
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
    try {
      return function.apply(args, bindings);
    }
    catch (ExpressionValidationException e) {
      // ExpressionValidationException already contain function name
      throw DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build(e, e.getMessage());
    }
    catch (Types.InvalidCastException | Types.InvalidCastBooleanException e) {
      throw DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build(e, "Function[%s] encountered exception: %s", name, e.getMessage());
    }
    catch (DruidException e) {
      throw e;
    }
    catch (Exception e) {
      throw DruidException.defensive().build(e, "Function[%s] encountered unknown exception.", name);
    }
  }

  @Override
  public boolean canVectorize(InputBindingInspector inspector)
  {
    return function.canVectorize(inspector, args);
  }

  @Override
  public ExprVectorProcessor<?> asVectorProcessor(VectorInputBindingInspector inspector)
  {
    return function.asVectorProcessor(inspector, args);
  }

  @Override
  public String stringify()
  {
    return StringUtils.format("%s(%s)", name, ARG_JOINER.join(args.stream().map(Expr::stringify).iterator()));
  }

  @Override
  public Expr visit(Shuttle shuttle)
  {
    return shuttle.visit(new FunctionExpr(function, name, shuttle.visitAll(args)));
  }

  @Override
  public BindingAnalysis analyzeInputs()
  {
    BindingAnalysis accumulator = new BindingAnalysis();

    for (Expr arg : args) {
      accumulator = accumulator.with(arg);
    }
    return accumulator.withScalarArguments(function.getScalarInputs(args))
                      .withArrayArguments(function.getArrayInputs(args))
                      .withArrayInputs(function.hasArrayInputs())
                      .withArrayOutput(function.hasArrayOutput());
  }

  @Override
  public ExpressionType getOutputType(InputBindingInspector inspector)
  {
    return function.getOutputType(inspector, args);
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
@SuppressWarnings("ClassName")
class ApplyFunctionExpr implements Expr
{
  final ApplyFunction function;
  final String name;
  final LambdaExpr lambdaExpr;
  final ImmutableList<Expr> argsExpr;
  final BindingAnalysis bindingAnalysis;
  final BindingAnalysis lambdaBindingAnalysis;
  final ImmutableList<BindingAnalysis> argsBindingAnalyses;

  ApplyFunctionExpr(ApplyFunction function, String name, LambdaExpr expr, List<Expr> args)
  {
    this.function = function;
    this.name = name;
    this.argsExpr = ImmutableList.copyOf(args);
    this.lambdaExpr = expr;

    function.validateArguments(expr, args);

    // apply function expressions are examined during expression selector creation, so precompute and cache the
    // binding details of children
    ImmutableList.Builder<BindingAnalysis> argBindingDetailsBuilder = ImmutableList.builder();
    BindingAnalysis accumulator = new BindingAnalysis();
    for (Expr arg : argsExpr) {
      BindingAnalysis argDetails = arg.analyzeInputs();
      argBindingDetailsBuilder.add(argDetails);
      accumulator = accumulator.with(argDetails);
    }

    lambdaBindingAnalysis = lambdaExpr.analyzeInputs();

    bindingAnalysis = accumulator.with(lambdaBindingAnalysis)
                                 .withArrayArguments(function.getArrayInputs(argsExpr))
                                 .withArrayInputs(true)
                                 .withArrayOutput(function.hasArrayOutput(lambdaExpr));
    argsBindingAnalyses = argBindingDetailsBuilder.build();
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
  public boolean canVectorize(InputBindingInspector inspector)
  {
    return function.canVectorize(inspector, lambdaExpr, argsExpr) &&
           lambdaExpr.canVectorize(inspector) &&
           argsExpr.stream().allMatch(expr -> expr.canVectorize(inspector));
  }

  @Override
  public <T> ExprVectorProcessor<T> asVectorProcessor(VectorInputBindingInspector inspector)
  {
    return function.asVectorProcessor(inspector, lambdaExpr, argsExpr);
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
  public Expr visit(Shuttle shuttle)
  {
    LambdaExpr newLambda = (LambdaExpr) lambdaExpr.visit(shuttle);
    return shuttle.visit(new ApplyFunctionExpr(function, name, newLambda, shuttle.visitAll(argsExpr)));
  }

  @Override
  public BindingAnalysis analyzeInputs()
  {
    return bindingAnalysis;
  }

  @Nullable
  @Override
  public ExpressionType getOutputType(InputBindingInspector inspector)
  {
    return function.getOutputType(inspector, lambdaExpr, argsExpr);
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
