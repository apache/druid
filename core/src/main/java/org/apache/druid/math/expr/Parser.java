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
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.antlr.ExprLexer;
import org.apache.druid.math.expr.antlr.ExprParser;

import javax.annotation.Nullable;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
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
          log.info("failed to instantiate " + clazz.getName() + ".. ignoring", e);
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
          log.info("failed to instantiate " + clazz.getName() + ".. ignoring", e);
        }
      }
    }
    APPLY_FUNCTIONS = ImmutableMap.copyOf(applyFunctionMap);
  }

  public static Function getFunction(String name)
  {
    return FUNCTIONS.get(StringUtils.toLowerCase(name));
  }

  public static ApplyFunction getApplyFunction(String name)
  {
    return APPLY_FUNCTIONS.get(StringUtils.toLowerCase(name));
  }

  public static Expr parse(String in, ExprMacroTable macroTable)
  {
    return parse(in, macroTable, true);
  }

  @VisibleForTesting
  static Expr parse(String in, ExprMacroTable macroTable, boolean withFlatten)
  {
    ExprLexer lexer = new ExprLexer(new ANTLRInputStream(in));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    ExprParser parser = new ExprParser(tokens);
    parser.setBuildParseTree(true);
    ParseTree parseTree = parser.expr();
    ParseTreeWalker walker = new ParseTreeWalker();
    ExprListenerImpl listener = new ExprListenerImpl(parseTree, macroTable);
    walker.walk(listener, parseTree);
    return withFlatten ? flatten(listener.getAST()) : listener.getAST();
  }

  public static Expr flatten(Expr expr)
  {
    if (expr instanceof BinaryOpExprBase) {
      BinaryOpExprBase binary = (BinaryOpExprBase) expr;
      Expr left = flatten(binary.left);
      Expr right = flatten(binary.right);
      if (Evals.isAllConstants(left, right)) {
        expr = expr.eval(null).toExpr();
      } else if (left != binary.left || right != binary.right) {
        return Evals.binaryOp(binary, left, right);
      }
    } else if (expr instanceof UnaryExpr) {
      UnaryExpr unary = (UnaryExpr) expr;
      Expr eval = flatten(unary.expr);
      if (eval instanceof ConstantExpr) {
        expr = expr.eval(null).toExpr();
      } else if (eval != unary.expr) {
        if (expr instanceof UnaryMinusExpr) {
          expr = new UnaryMinusExpr(eval);
        } else if (expr instanceof UnaryNotExpr) {
          expr = new UnaryNotExpr(eval);
        } else {
          expr = unary; // unknown type..
        }
      }
    } else if (expr instanceof FunctionExpr) {
      FunctionExpr functionExpr = (FunctionExpr) expr;
      List<Expr> args = functionExpr.args;
      boolean flattened = false;
      List<Expr> flattening = Lists.newArrayListWithCapacity(args.size());
      for (Expr arg : args) {
        Expr flatten = flatten(arg);
        flattened |= flatten != arg;
        flattening.add(flatten);
      }
      if (Evals.isAllConstants(flattening)) {
        expr = expr.eval(null).toExpr();
      } else if (flattened) {
        expr = new FunctionExpr(functionExpr.function, functionExpr.name, flattening);
      }
    }
    return expr;
  }

  public static Expr applyUnappliedIdentifiers(Expr expr, List<String> unapplied)
  {
    Preconditions.checkArgument(unapplied.size() > 0);

    // special handle if expr is just array identifier or array is being directly cast, that doesn't count
    String s = Parser.getIdentifierOrCastIdentifier(expr);
    if (s != null) {
      return expr;
    }

    ApplyFunction fn;
    final LambdaExpr lambdaExpr;
    final List<Expr> args;

    // any unapplied identifiers that are inside a lambda expression need that lambda expression to be rewritten
    Expr newExpr = expr.visit(
        new Expr.Shuttle()
        {
          @Override
          public Expr visit(Expr expr)
          {
            if (expr instanceof ApplyFunctionExpr) {
              return liftApplyLambda((ApplyFunctionExpr) expr, unapplied);
            }
            return expr;
          }
        }
    );
    final Set<String> expectedArrays = Parser.findArrayFnBindings(newExpr);
    List<String> remainingUnappliedArgs =
        unapplied.stream().filter(x -> !expectedArrays.contains(x)).collect(Collectors.toList());

    // if lifting the lambdas got rid of all missing bindings, return the transformed expression
    if (remainingUnappliedArgs.size() == 0) {
      return newExpr;
    }

    // else, it *should be safe* to wrap in either map or cartesian_map because we still have missing bindings that
    // were *not* referenced in a lambda body
    if (remainingUnappliedArgs.size() == 1) {
      fn = new ApplyFunction.MapFunction();
      IdentifierExpr lambdaArg = new IdentifierExpr(remainingUnappliedArgs.iterator().next());
      lambdaExpr = new LambdaExpr(ImmutableList.of(lambdaArg), expr);
      args = ImmutableList.of(lambdaArg);
    } else {
      fn = new ApplyFunction.CartesianMapFunction();
      List<IdentifierExpr> identifiers = new ArrayList<>(remainingUnappliedArgs.size());
      args = new ArrayList<>(remainingUnappliedArgs.size());
      for (String remainingUnappliedArg : remainingUnappliedArgs) {
        IdentifierExpr arg = new IdentifierExpr(remainingUnappliedArg);
        identifiers.add(arg);
        args.add(arg);
      }
      lambdaExpr = new LambdaExpr(identifiers, expr);
    }

    Expr magic = new ApplyFunctionExpr(fn, fn.name(), lambdaExpr, args);
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
    // this will _not_ include the lambda identifiers.. anything in this list needs to be applied
    List<IdentifierExpr> unappliedLambdaBindings = Parser.findRequiredBindings(expr.lambdaExpr)
                                                         .stream()
                                                         .filter(unappliedArgs::contains)
                                                         .map(IdentifierExpr::new)
                                                         .collect(Collectors.toList());

    if (unappliedLambdaBindings.size() == 0) {
      return expr;
    }

    final ApplyFunction newFn;
    final ApplyFunctionExpr newExpr;

    final List<Expr> newArgs = new ArrayList<>(expr.argsExpr);
    newArgs.addAll(unappliedLambdaBindings);

    switch (expr.function.name()) {
      case ApplyFunction.MapFunction.NAME:
      case ApplyFunction.CartesianMapFunction.NAME:
        // map(x -> x + y, x) => cartesian_map((x, y) -> x + y, x, y)
        // cartesian_map((x, y) -> x + y + z, x, y) => cartesian_map((x, y, z) -> x + y + z, x, y, z)
        final List<IdentifierExpr> lambdaIds = new ArrayList<>(expr.lambdaExpr.getIdentifiers().size() + unappliedArgs.size());
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
        // filter(x -> x > y, x) => filter(x -> x, cartesian_map((x,y) -> x > y, x, y))
        // any(x -> x > y, x) => any(x -> x, cartesian_map((x, y) -> x > y, x, y))
        // all(x -> x > y, x) => all(x -> x, cartesian_map((x, y) -> x > y, x, y))
        ApplyFunction newArrayFn = new ApplyFunction.CartesianMapFunction();
        IdentifierExpr identityExprIdentifier = new IdentifierExpr("_");
        LambdaExpr identityExpr = new LambdaExpr(ImmutableList.of(identityExprIdentifier), identityExprIdentifier);
        ApplyFunctionExpr arrayExpr = new ApplyFunctionExpr(newArrayFn, newArrayFn.name(), identityExpr, newArgs);
        newExpr = new ApplyFunctionExpr(expr.function, expr.function.name(), identityExpr, ImmutableList.of(arrayExpr));
        break;
      case ApplyFunction.FoldrFunction.NAME:
      case ApplyFunction.CartesianFoldrFunction.NAME:
        // foldr((x, acc) -> acc + x + y, x, acc) => cartesian_foldr((x, y, acc) -> acc + x + y, x, y, acc)
        // cartesian_foldr((x, y, acc) -> acc + x + y + z, x, y, acc) => cartesian_foldr((x, y, z, acc) -> acc + x + y + z, x, y, z, acc)

        final List<Expr> newFoldArgs = new ArrayList<>(expr.argsExpr.size() + unappliedArgs.size());
        final List<IdentifierExpr> newFoldLambdaIdentifiers = new ArrayList<>(expr.lambdaExpr.getIdentifiers().size() + unappliedArgs.size());
        final List<IdentifierExpr> existingFoldLambdaIdentifiers = expr.lambdaExpr.getIdentifierExprs();
        // accumulator argument is last argument, slice it off when constructing new arg list and lambda args identifiers
        for (int i = 0; i < expr.argsExpr.size() - 1; i++) {
          newFoldArgs.add(expr.argsExpr.get(i));
          newFoldLambdaIdentifiers.add(existingFoldLambdaIdentifiers.get(i));
        }
        newFoldArgs.addAll(unappliedLambdaBindings);
        newFoldLambdaIdentifiers.addAll(unappliedLambdaBindings);
        // add accumulator last
        newFoldLambdaIdentifiers.add(existingFoldLambdaIdentifiers.get(existingFoldLambdaIdentifiers.size() - 1));
        newArgs.addAll(unappliedLambdaBindings);
        final LambdaExpr newFoldLambda = new LambdaExpr(newFoldLambdaIdentifiers, expr.lambdaExpr.getExpr());

        newFn = new ApplyFunction.CartesianFoldrFunction();
        newExpr = new ApplyFunctionExpr(newFn, newFn.name(), newFoldLambda, newFoldArgs);
        break;
      default:
        throw new RE("Unable to transform apply function:[%s]", expr.function.name());
    }

    return newExpr;
  }

  public static List<String> findRequiredBindings(Expr expr)
  {
    final Set<String> found = new LinkedHashSet<>();
    expr.visit(
        new Expr.Visitor()
        {
          @Override
          public void visit(Expr expr)
          {
            if (expr instanceof IdentifierExpr) {
              found.add(expr.toString());
            } else if (expr instanceof LambdaExpr) {
              LambdaExpr lambda = (LambdaExpr) expr;
              for (String identifier : lambda.getIdentifiers()) {
                found.remove(identifier);
              }
            }
          }
        }
    );
    return Lists.newArrayList(found);
  }

  public static Set<String> findArrayFnBindings(Expr expr)
  {
    final Set<String> arrayFnBindings = new LinkedHashSet<>();
    expr.visit(new Expr.Visitor()
    {
      @Override
      public void visit(Expr expr)
      {
        final Set<Expr> arrayArgs;
        if (expr instanceof FunctionExpr && ((FunctionExpr) expr).function instanceof Function.ArrayFunction) {
          FunctionExpr fnExpr = (FunctionExpr) expr;
          Function.ArrayFunction fn = (Function.ArrayFunction) fnExpr.function;
          arrayArgs = fn.getArrayInputs(fnExpr.args);
        } else if (expr instanceof ApplyFunctionExpr) {
          ApplyFunctionExpr applyExpr = (ApplyFunctionExpr) expr;
          arrayArgs = applyExpr.function.getArrayInputs(applyExpr.argsExpr);
        } else {
          arrayArgs = Collections.emptySet();
        }
        for (Expr arg : arrayArgs) {
          String s = getIdentifierOrCastIdentifier(arg);
          if (s != null) {
            arrayFnBindings.add(s);
          }
        }
      }
    });
    return arrayFnBindings;
  }

  /**
   * Visits all nodes of an {@link Expr}, collecting information about how {@link IdentifierExpr} are used
   */
  public static BindingDetails examineBindings(Expr expr)
  {
    final Set<String> freeVariables = new HashSet<>();
    final Set<String> scalarVariables = new HashSet<>();
    final Set<String> arrayVariables = new HashSet<>();
    expr.visit(childExpr -> {
      if (childExpr instanceof IdentifierExpr) {
        // all identifiers are free variables ...
        freeVariables.add(childExpr.toString());
      } else if (childExpr instanceof LambdaExpr) {
        // ... unless they are erased by appearing in a lambda expression's arguments because they will be bound by
        // the apply expression that wraps the lambda
        LambdaExpr lambda = (LambdaExpr) childExpr;
        for (String identifier : lambda.getIdentifiers()) {
          freeVariables.remove(identifier);
          scalarVariables.remove(identifier);
          arrayVariables.remove(identifier);
        }
      } else {
        // shallowly examining function expressions and apply function expressions can give us some context about if
        // identifiers are used as scalar or array arguments to these functions. all identifiers should be encountered
        // at some point, so we can use this to validate that identifiers are not used in inconsistent ways
        final Set<Expr> scalarArgs;
        final Set<Expr> arrayArgs;
        if (childExpr instanceof FunctionExpr) {
          FunctionExpr fnExpr = (FunctionExpr) childExpr;
          scalarArgs = fnExpr.function.getScalarInputs(fnExpr.args);

          if (fnExpr.function instanceof Function.ArraysFunction) {
            Function.ArrayFunction fn = (Function.ArrayFunction) fnExpr.function;
            arrayArgs = fn.getArrayInputs(fnExpr.args);
          } else {
            arrayArgs = Collections.emptySet();
          }
        } else if (childExpr instanceof ApplyFunctionExpr) {
          ApplyFunctionExpr applyExpr = (ApplyFunctionExpr) childExpr;
          scalarArgs = Collections.emptySet();
          arrayArgs = applyExpr.function.getArrayInputs(applyExpr.argsExpr);
        } else if (childExpr instanceof BinaryOpExprBase) {
          BinaryOpExprBase binExpr = (BinaryOpExprBase) childExpr;
          scalarArgs = ImmutableSet.of(binExpr.left, binExpr.right);
          arrayArgs = Collections.emptySet();
        } else if (childExpr instanceof UnaryExpr) {
          UnaryExpr unaryExpr = (UnaryExpr) childExpr;
          scalarArgs = ImmutableSet.of(unaryExpr.expr);
          arrayArgs = Collections.emptySet();
        } else {
          // bail, child expression is not a function, apply function, or operator, nothing for us here
          return;
        }
        for (Expr arg : scalarArgs) {
          String s = getIdentifierIfIdentifier(arg);
          if (s != null) {
            scalarVariables.add(s);
          }
        }
        for (Expr arg : arrayArgs) {
          String s = getIdentifierOrCastIdentifier(arg);
          if (s != null) {
            arrayVariables.add(s);
          }
        }
      }
    });
    for (String identifier : scalarVariables) {
      if (arrayVariables.contains(identifier)) {
        throw new RE("Invalid expression: %s; identifier [%s] used as both scalar and array", expr, identifier);
      }
    }
    return new BindingDetails(freeVariables, scalarVariables, arrayVariables);
  }

  @Nullable
  public static String getIdentifierOrCastIdentifier(Expr expr)
  {
    if (expr instanceof IdentifierExpr) {
      return expr.toString();
    } else if (expr instanceof FunctionExpr && ((FunctionExpr) expr).function instanceof Function.CastFunc) {
      FunctionExpr fn = (FunctionExpr) expr;
      return getIdentifierOrCastIdentifier(fn.args.get(0));
    }
    return null;
  }

  @Nullable
  public static String getIdentifierIfIdentifier(Expr expr)
  {
    if (expr instanceof IdentifierExpr) {
      return expr.toString();
    } else {
      return null;
    }
  }

  public static Expr.ObjectBinding withMap(final Map<String, ?> bindings)
  {
    return bindings::get;
  }

  public static Expr.ObjectBinding withSuppliers(final Map<String, Supplier<Object>> bindings)
  {
    return (String name) -> {
      Supplier<Object> supplier = bindings.get(name);
      return supplier == null ? null : supplier.get();
    };
  }

  public static class BindingDetails
  {
    private final Set<String> freeVariables;
    private final Set<String> scalarVariables;
    private final Set<String> arrayVariables;

    BindingDetails(Set<String> freeVariables, Set<String> scalarVariables, Set<String> arrayVariables)
    {
      this.freeVariables = freeVariables;
      this.scalarVariables = scalarVariables;
      this.arrayVariables = arrayVariables;
    }

    public List<String> getRequiredColumns()
    {
      return new ArrayList<>(freeVariables);
    }

    public Set<String> getFreeVariables()
    {
      return freeVariables;
    }

    public Set<String> getScalarVariables()
    {
      return scalarVariables;
    }

    public Set<String> getArrayVariables()
    {
      return arrayVariables;
    }
  }
}
