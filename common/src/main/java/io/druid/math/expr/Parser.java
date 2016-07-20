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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.logger.Logger;
import io.druid.math.expr.Function.FunctionFactory;
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

  private static final Map<String, Supplier<Function>> functions = Maps.newHashMap();

  static {
    register(BuiltinFunctions.class);
  }

  public static void register(Class parent)
  {
    for (Class clazz : parent.getClasses()) {
      if (!Modifier.isAbstract(clazz.getModifiers()) && Function.class.isAssignableFrom(clazz)) {
        try {
          Function function = (Function) clazz.newInstance();
          String name = function.name().toLowerCase();
          if (functions.containsKey(name)) {
            throw new IllegalArgumentException("function '" + name + "' should not be overridden");
          }
          Supplier<Function> supplier = function instanceof FunctionFactory ? (FunctionFactory) function
                                                                            : Suppliers.ofInstance(function);
          functions.put(name, supplier);
          if (parent != BuiltinFunctions.class) {
            log.info("user defined function '" + name + "' is registered with class " + clazz.getName());
          }
        }
        catch (Exception e) {
          log.info(e, "failed to instantiate " + clazz.getName() + ".. ignoring");
        }
      }
    }
  }

  public static Function getFunction(String name) {
    Supplier<Function> supplier = functions.get(name.toLowerCase());
    if (supplier == null) {
      throw new IAE("Invalid function name '%s'", name);
    }
    return supplier.get();
  }

  public static boolean hasFunction(String name)
  {
    return functions.containsKey(name.toLowerCase());
  }

  public static Expr parse(String in)
  {
    return parse(in, true);
  }
  public static Expr parse(String in, boolean withFlatten)
  {
    ParseTree parseTree = parseTree(in);
    ParseTreeWalker walker = new ParseTreeWalker();
    ExprListenerImpl listener = new ExprListenerImpl(parseTree);
    walker.walk(listener, parseTree);
    return withFlatten ? flatten(listener.getAST()) : listener.getAST();
  }

  public static ParseTree parseTree(String in)
  {
    ExprLexer lexer = new ExprLexer(new ANTLRInputStream(in));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    ExprParser parser = new ExprParser(tokens);
    parser.setBuildParseTree(true);
    return parser.expr();
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
        expr = new FunctionExpr(functionExpr.name, functionExpr.func, flattening);
      }
    }
    return expr;
  }

  public static List<String> findRequiredBindings(String in)
  {
    return findRequiredBindings(parse(in));
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
    return new Expr.ObjectBinding()
    {
      @Override
      public Number get(String name)
      {
        Number number = (Number)bindings.get(name);
        if (number == null && !bindings.containsKey(name)) {
          throw new RuntimeException("No binding found for " + name);
        }
        return number;
      }
    };
  }

  public static Expr.ObjectBinding withSuppliers(final Map<String, Supplier<Number>> bindings)
  {
    return new Expr.ObjectBinding()
    {
      @Override
      public Number get(String name)
      {
        Supplier<Number> supplier = bindings.get(name);
        return supplier == null ? null : supplier.get();
      }
    };
  }
}
