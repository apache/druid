/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.math.expr;


import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.math.expr.antlr.ExprLexer;
import io.druid.math.expr.antlr.ExprParser;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Parser
{
  private static final Logger log = new Logger(Parser.class);
  private static final Map<String, Function> FUNCTIONS;

  static {
    Map<String, Function> functionMap = Maps.newHashMap();
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
  }

  public static Function getFunction(String name)
  {
    return FUNCTIONS.get(StringUtils.toLowerCase(name));
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
      for (int i = 0; i < args.size(); i++) {
        Expr flatten = flatten(args.get(i));
        flattened |= flatten != args.get(i);
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

  public static List<String> findRequiredBindings(Expr expr)
  {
    final Set<String> found = Sets.newLinkedHashSet();
    expr.visit(
        new Expr.Visitor()
        {
          @Override
          public void visit(Expr expr)
          {
            if (expr instanceof IdentifierExpr) {
              found.add(expr.toString());
            }
          }
        }
    );
    return Lists.newArrayList(found);
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
}
