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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.metamx.common.logger.Logger;
import com.google.common.collect.Lists;
import io.druid.math.expr.antlr.ExprLexer;
import io.druid.math.expr.antlr.ExprParser;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;

public class Parser
{
  static final Logger log = new Logger(Parser.class);
  static final Map<String, Function> func;

  static {
    Map<String, Function> functionMap = Maps.newHashMap();
    for (Class clazz : Function.class.getClasses()) {
      if (!Modifier.isAbstract(clazz.getModifiers()) && Function.class.isAssignableFrom(clazz)) {
        try {
          Function function = (Function)clazz.newInstance();
          functionMap.put(function.name().toLowerCase(), function);
        }
        catch (Exception e) {
          log.info("failed to instantiate " + clazz.getName() + ".. ignoring", e);
        }
      }
    }
    func = ImmutableMap.copyOf(functionMap);
  }

  public static Expr parse(String in)
  {
    ExprLexer lexer = new ExprLexer(new ANTLRInputStream(in));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    ExprParser parser = new ExprParser(tokens);
    parser.setBuildParseTree(true);
    ParseTree parseTree = parser.expr();
    ParseTreeWalker walker = new ParseTreeWalker();
    ExprListenerImpl listener = new ExprListenerImpl(parseTree);
    walker.walk(listener, parseTree);
    return listener.getAST();
  }

  public static List<String> findRequiredBindings(String in)
  {
    return findRequiredBindings(parse(in));
  }

  public static List<String> findRequiredBindings(Expr parsed)
  {
    return findRecursive(parsed, Lists.<String>newArrayList());
  }

  private static List<String> findRecursive(Expr expr, List<String> found)
  {
    if (expr instanceof IdentifierExpr) {
      found.add(expr.toString());
    } else if (expr instanceof BinaryOpExprBase) {
      BinaryOpExprBase binary = (BinaryOpExprBase) expr;
      findRecursive(binary.left, found);
      findRecursive(binary.right, found);
    } else if (expr instanceof UnaryMinusExpr) {
      findRecursive(((UnaryMinusExpr) expr).expr, found);
    } else if (expr instanceof UnaryNotExpr) {
      findRecursive(((UnaryNotExpr) expr).expr, found);
    } else if (expr instanceof FunctionExpr) {
      for (Expr child : ((FunctionExpr) expr).args) {
        findRecursive(child, found);
      }
    }
    return found;
  }

  public static Expr.NumericBinding withMap(final Map<String, ?> bindings)
  {
    return new Expr.NumericBinding()
    {
      @Override
      public Number get(String name)
      {
        return (Number)bindings.get(name);
      }
    };
  }

  public static Expr.NumericBinding withSuppliers(final Map<String, Supplier<Number>> bindings)
  {
    return new Expr.NumericBinding()
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
