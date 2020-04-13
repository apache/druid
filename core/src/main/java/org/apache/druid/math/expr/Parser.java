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


import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.antlr.ExprLexer;
import org.apache.druid.math.expr.antlr.ExprParser;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Parser
{
  private static final Logger log = new Logger(Parser.class);
  private static final Map<String, Function> FUNCTIONS;
  private static final Map<String, ApplyFunction> APPLY_FUNCTIONS;

  static {
    Map<String, Function> functionMap = new HashMap<>();
    for (Class clazz : Function.class.getClasses()) {
      if (!Modifier.isAbstract(clazz.getModifiers()) && Function.class.isAssignableFrom(clazz)) {
        try {
          Function function = (Function) clazz.newInstance();
          functionMap.put(StringUtils.toLowerCase(function.name()), function);
        }
        catch (Exception e) {
          log.error(e, "failed to instantiate %s.. ignoring", clazz.getName());
        }
      }
    }
    FUNCTIONS = ImmutableMap.copyOf(functionMap);

    Map<String, ApplyFunction> applyFunctionMap = new HashMap<>();
    for (Class clazz : ApplyFunction.class.getClasses()) {
      if (!Modifier.isAbstract(clazz.getModifiers()) && ApplyFunction.class.isAssignableFrom(clazz)) {
        try {
          ApplyFunction function = (ApplyFunction) clazz.newInstance();
          applyFunctionMap.put(StringUtils.toLowerCase(function.name()), function);
        }
        catch (Exception e) {
          log.error(e, "failed to instantiate %s.. ignoring", clazz.getName());
        }
      }
    }
    APPLY_FUNCTIONS = ImmutableMap.copyOf(applyFunctionMap);
  }

  /**
   * Get {@link Function} by {@link Function#name()}
   */
  public static Function getFunction(String name)
  {
    return FUNCTIONS.get(StringUtils.toLowerCase(name));
  }

  /**
   * Get {@link ApplyFunction} by {@link ApplyFunction#name()}
   */
  public static ApplyFunction getApplyFunction(String name)
  {
    return APPLY_FUNCTIONS.get(StringUtils.toLowerCase(name));
  }

  /**
   * Parse a string into a flattened {@link Expr}. There is some overhead to this, and these objects are all immutable,
   * so re-use instead of re-creating whenever possible.
   * @param in expression to parse
   * @param macroTable additional extensions to expression language
   * @return
   */
  public static Expr parse(String in, ExprMacroTable macroTable)
  {
    return parse(in, macroTable, true);
  }

  @VisibleForTesting
  public static Expr parse(String in, ExprMacroTable macroTable, boolean withFlatten)
  {
    ExprLexer lexer = new ExprLexer(new ANTLRInputStream(in));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    ExprParser parser = new ExprParser(tokens);
    parser.setBuildParseTree(true);
    ParseTree parseTree = parser.expr();
    ParseTreeWalker walker = new ParseTreeWalker();
    ExprListenerImpl listener = new ExprListenerImpl(parseTree, macroTable);
    walker.walk(listener, parseTree);
    Expr parsed = listener.getAST();
    if (parsed == null) {
      throw new RE("Failed to parse expression: %s", in);
    }
    return withFlatten ? flatten(parsed) : parsed;
  }

  /**
   * Flatten an {@link Expr}, evaluating expressions on constants where possible to simplify the {@link Expr}.
   */
  public static Expr flatten(Expr expr)
  {
    return expr.visit(childExpr -> {
      if (childExpr instanceof BinaryOpExprBase) {
        BinaryOpExprBase binary = (BinaryOpExprBase) childExpr;
        if (Evals.isAllConstants(binary.left, binary.right)) {
          return childExpr.eval(null).toExpr();
        }
      } else if (childExpr instanceof UnaryExpr) {
        UnaryExpr unary = (UnaryExpr) childExpr;

        if (unary.expr instanceof ConstantExpr) {
          return childExpr.eval(null).toExpr();
        }
      } else if (childExpr instanceof FunctionExpr) {
        FunctionExpr functionExpr = (FunctionExpr) childExpr;
        List<Expr> args = functionExpr.args;
        if (Evals.isAllConstants(args)) {
          return childExpr.eval(null).toExpr();
        }
      } else if (childExpr instanceof ApplyFunctionExpr) {
        ApplyFunctionExpr applyFunctionExpr = (ApplyFunctionExpr) childExpr;
        List<Expr> args = applyFunctionExpr.argsExpr;
        if (Evals.isAllConstants(args)) {
          if (applyFunctionExpr.analyzeInputs().getFreeVariables().size() == 0) {
            return childExpr.eval(null).toExpr();
          }
        }
      }
      return childExpr;
    });
  }

  /**
   * Applies a transformation to an {@link Expr} given a list of known (or uknown) multi-value input columns that are
   * used in a scalar manner, walking the {@link Expr} tree and lifting array variables into the {@link LambdaExpr} of
   * {@link ApplyFunctionExpr} and transforming the arguments of {@link FunctionExpr}
   * @param expr expression to visit and rewrite
   * @param bindingsToApply
   * @return
   */
  public static Expr applyUnappliedBindings(Expr expr, Expr.BindingDetails bindingDetails, List<String> bindingsToApply)
  {
    if (bindingsToApply.isEmpty()) {
      // nothing to do, expression is fine as is
      return expr;
    }
    // filter the list of bindings to those which are used in this expression
    List<String> unappliedBindingsInExpression = bindingsToApply.stream()
                                     .filter(x -> bindingDetails.getRequiredBindings().contains(x))
                                     .collect(Collectors.toList());

    // any unapplied bindings that are inside a lambda expression need that lambda expression to be rewritten
    Expr newExpr = expr.visit(
        childExpr -> {
          if (childExpr instanceof ApplyFunctionExpr) {
            // try to lift unapplied arguments into the apply function lambda
            return liftApplyLambda((ApplyFunctionExpr) childExpr, unappliedBindingsInExpression);
          } else if (childExpr instanceof FunctionExpr) {
            // check array function arguments for unapplied identifiers to transform if necessary
            FunctionExpr fnExpr = (FunctionExpr) childExpr;
            Set<Expr> arrayInputs = fnExpr.function.getArrayInputs(fnExpr.args);
            List<Expr> newArgs = new ArrayList<>();
            for (Expr arg : fnExpr.args) {
              if (arg.getIdentifierIfIdentifier() == null && arrayInputs.contains(arg)) {
                Expr newArg = applyUnappliedBindings(arg, bindingDetails, unappliedBindingsInExpression);
                newArgs.add(newArg);
              } else {
                newArgs.add(arg);
              }
            }

            FunctionExpr newFnExpr = new FunctionExpr(fnExpr.function, fnExpr.function.name(), newArgs);
            return newFnExpr;
          }
          return childExpr;
        }
    );

    Expr.BindingDetails newExprBindings = newExpr.analyzeInputs();
    final Set<String> expectedArrays = newExprBindings.getArrayVariables();

    List<String> remainingUnappliedBindings =
        unappliedBindingsInExpression.stream().filter(x -> !expectedArrays.contains(x)).collect(Collectors.toList());

    // if lifting the lambdas got rid of all missing bindings, return the transformed expression
    if (remainingUnappliedBindings.isEmpty()) {
      return newExpr;
    }

    return applyUnapplied(newExpr, remainingUnappliedBindings);
  }

  /**
   * translate an {@link Expr} into an {@link ApplyFunctionExpr} for {@link ApplyFunction.MapFunction} or
   * {@link ApplyFunction.CartesianMapFunction} if there are multiple unbound arguments to be applied
   */
  private static Expr applyUnapplied(Expr expr, List<String> unappliedBindings)
  {
    // filter to get list of IdentifierExpr that are backed by the unapplied bindings
    final List<IdentifierExpr> args = expr.analyzeInputs()
                                          .getFreeVariables()
                                          .stream()
                                          .filter(x -> unappliedBindings.contains(x.getBinding()))
                                          .collect(Collectors.toList());

    final List<IdentifierExpr> lambdaArgs = new ArrayList<>();

    // construct lambda args from list of args to apply. Identifiers in a lambda body have artificial 'binding' values
    // that is the same as the 'identifier', because the bindings are supplied by the wrapping apply function
    // replacements are done by binding rather than identifier because repeats of the same input should not result
    // in a cartesian product
    final Map<String, IdentifierExpr> toReplace = new HashMap<>();
    for (IdentifierExpr applyFnArg : args) {
      if (!toReplace.containsKey(applyFnArg.getBinding())) {
        IdentifierExpr lambdaRewrite = new IdentifierExpr(applyFnArg.getBinding());
        lambdaArgs.add(lambdaRewrite);
        toReplace.put(applyFnArg.getBinding(), lambdaRewrite);
      }
    }

    // rewrite identifiers in the expression which will become the lambda body, so they match the lambda identifiers we
    // are constructing
    Expr newExpr = expr.visit(childExpr -> {
      if (childExpr instanceof IdentifierExpr) {
        if (toReplace.containsKey(((IdentifierExpr) childExpr).getBinding())) {
          return toReplace.get(((IdentifierExpr) childExpr).getBinding());
        }
      }
      return childExpr;
    });


    // wrap an expression in either map or cartesian_map to apply any unapplied identifiers
    final LambdaExpr lambdaExpr = new LambdaExpr(lambdaArgs, newExpr);
    final ApplyFunction fn;
    if (lambdaArgs.size() == 1) {
      fn = new ApplyFunction.MapFunction();
    } else {
      fn = new ApplyFunction.CartesianMapFunction();
    }

    final Expr magic = new ApplyFunctionExpr(fn, fn.name(), lambdaExpr, ImmutableList.copyOf(lambdaArgs));
    return magic;
  }

  /**
   * Performs partial lifting of free identifiers of the lambda expression of an {@link ApplyFunctionExpr}, constrained
   * by a list of "unapplied" identifiers, and translating them into arguments of a new {@link LambdaExpr} and
   * {@link ApplyFunctionExpr} as appropriate.
   *
   * The "unapplied" identifiers list is used to allow say only lifting array identifiers and adding it to the cartesian
   * product to allow "magical" translation of multi-value string dimensions which are expressed as single value
   * dimensions to function correctly and as expected.
   */
  private static ApplyFunctionExpr liftApplyLambda(ApplyFunctionExpr expr, List<String> unappliedArgs)
  {
    // recursively evaluate arguments to ensure they are properly transformed into arrays as necessary
    Set<String> unappliedInThisApply =
        unappliedArgs.stream()
                     .filter(u -> !expr.bindingDetails.getArrayBindings().contains(u))
                     .collect(Collectors.toSet());

    List<String> unappliedIdentifiers =
        expr.bindingDetails
            .getFreeVariables()
            .stream()
            .filter(x -> unappliedInThisApply.contains(x.getBindingIfIdentifier()))
            .map(IdentifierExpr::getIdentifierIfIdentifier)
            .collect(Collectors.toList());

    List<Expr> newArgs = new ArrayList<>();
    for (int i = 0; i < expr.argsExpr.size(); i++) {
      newArgs.add(
          applyUnappliedBindings(
              expr.argsExpr.get(i),
              expr.argsBindingDetails.get(i),
              unappliedIdentifiers
          )
      );
    }

    // this will _not_ include the lambda identifiers.. anything in this list needs to be applied
    List<IdentifierExpr> unappliedLambdaBindings =
        expr.lambdaBindingDetails.getFreeVariables()
                                 .stream()
                                 .filter(x -> unappliedArgs.contains(x.getBindingIfIdentifier()))
                                 .map(x -> new IdentifierExpr(x.getIdentifier(), x.getBinding()))
                                 .collect(Collectors.toList());

    if (unappliedLambdaBindings.isEmpty()) {
      return new ApplyFunctionExpr(expr.function, expr.name, expr.lambdaExpr, newArgs);
    }

    final ApplyFunction newFn;
    final ApplyFunctionExpr newExpr;

    newArgs.addAll(unappliedLambdaBindings);

    switch (expr.function.name()) {
      case ApplyFunction.MapFunction.NAME:
      case ApplyFunction.CartesianMapFunction.NAME:
        // map(x -> x + y, x) =>
        //  cartesian_map((x, y) -> x + y, x, y)
        // cartesian_map((x, y) -> x + y + z, x, y) =>
        //  cartesian_map((x, y, z) -> x + y + z, x, y, z)
        final List<IdentifierExpr> lambdaIds =
            new ArrayList<>(expr.lambdaExpr.getIdentifiers().size() + unappliedArgs.size());
        lambdaIds.addAll(expr.lambdaExpr.getIdentifierExprs());
        lambdaIds.addAll(unappliedLambdaBindings);
        final LambdaExpr newLambda = new LambdaExpr(lambdaIds, expr.lambdaExpr.getExpr());
        newFn = new ApplyFunction.CartesianMapFunction();
        newExpr = new ApplyFunctionExpr(newFn, newFn.name(), newLambda, newArgs);
        break;
      case ApplyFunction.AllMatchFunction.NAME:
      case ApplyFunction.AnyMatchFunction.NAME:
      case ApplyFunction.FilterFunction.NAME:
        // i'm lazy and didn't add 'cartesian_filter', 'cartesian_any', and 'cartesian_and', so instead steal the match
        // expressions lambda and translate it into a 'cartesian_map', and apply that to the match function with a new
        // identity expression lambda since the input is an array of boolean expression results (or should be..)
        // filter(x -> x > y, x) =>
        //  filter(x -> x, cartesian_map((x,y) -> x > y, x, y))
        // any(x -> x > y, x) =>
        //  any(x -> x, cartesian_map((x, y) -> x > y, x, y))
        // all(x -> x > y, x) =>
        //  all(x -> x, cartesian_map((x, y) -> x > y, x, y))
        ApplyFunction newArrayFn = new ApplyFunction.CartesianMapFunction();
        IdentifierExpr identityExprIdentifier = new IdentifierExpr("_");
        LambdaExpr identityExpr = new LambdaExpr(ImmutableList.of(identityExprIdentifier), identityExprIdentifier);
        ApplyFunctionExpr arrayExpr = new ApplyFunctionExpr(newArrayFn, newArrayFn.name(), identityExpr, newArgs);
        newExpr = new ApplyFunctionExpr(expr.function, expr.function.name(), identityExpr, ImmutableList.of(arrayExpr));
        break;
      case ApplyFunction.FoldFunction.NAME:
      case ApplyFunction.CartesianFoldFunction.NAME:
        // fold((x, acc) -> acc + x + y, x, acc) =>
        //  cartesian_fold((x, y, acc) -> acc + x + y, x, y, acc)
        // cartesian_fold((x, y, acc) -> acc + x + y + z, x, y, acc) =>
        //  cartesian_fold((x, y, z, acc) -> acc + x + y + z, x, y, z, acc)

        final List<Expr> newFoldArgs =
            new ArrayList<>(expr.argsExpr.size() + unappliedLambdaBindings.size());
        final List<IdentifierExpr> newFoldLambdaIdentifiers =
            new ArrayList<>(expr.lambdaExpr.getIdentifiers().size() + unappliedLambdaBindings.size());
        final List<IdentifierExpr> existingFoldLambdaIdentifiers = expr.lambdaExpr.getIdentifierExprs();
        // accumulator argument is last argument, slice it off when constructing new arg list and lambda args
        for (int i = 0; i < expr.argsExpr.size() - 1; i++) {
          newFoldArgs.add(expr.argsExpr.get(i));
          newFoldLambdaIdentifiers.add(existingFoldLambdaIdentifiers.get(i));
        }
        newFoldArgs.addAll(unappliedLambdaBindings);
        newFoldLambdaIdentifiers.addAll(unappliedLambdaBindings);
        // add accumulator last
        newFoldLambdaIdentifiers.add(existingFoldLambdaIdentifiers.get(existingFoldLambdaIdentifiers.size() - 1));
        newFoldArgs.add(expr.argsExpr.get(expr.argsExpr.size() - 1));
        final LambdaExpr newFoldLambda = new LambdaExpr(newFoldLambdaIdentifiers, expr.lambdaExpr.getExpr());

        newFn = new ApplyFunction.CartesianFoldFunction();
        newExpr = new ApplyFunctionExpr(newFn, newFn.name(), newFoldLambda, newFoldArgs);
        break;
      default:
        throw new RE("Unable to transform apply function:[%s]", expr.function.name());
    }

    return newExpr;
  }

  /**
   * Validate that an expression uses input bindings in a type consistent manner.
   */
  public static void validateExpr(Expr expression, Expr.BindingDetails bindingDetails)
  {
    final Set<String> conflicted =
        Sets.intersection(bindingDetails.getScalarBindings(), bindingDetails.getArrayBindings());
    if (!conflicted.isEmpty()) {
      throw new RE("Invalid expression: %s; %s used as both scalar and array variables", expression, conflicted);
    }
  }

  /**
   * Create {@link Expr.ObjectBinding} backed by {@link Map} to provide values for identifiers to evaluate {@link Expr}
   */
  public static Expr.ObjectBinding withMap(final Map<String, ?> bindings)
  {
    return bindings::get;
  }

  /**
   * Create {@link Expr.ObjectBinding} backed by map of {@link Supplier} to provide values for identifiers to evaluate
   * {@link Expr}
   */
  public static Expr.ObjectBinding withSuppliers(final Map<String, Supplier<Object>> bindings)
  {
    return (String name) -> {
      Supplier<Object> supplier = bindings.get(name);
      return supplier == null ? null : supplier.get();
    };
  }
}
