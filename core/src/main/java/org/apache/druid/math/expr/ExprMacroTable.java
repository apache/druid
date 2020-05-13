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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Mechanism by which Druid expressions can define new functions for the Druid expression language. When
 * {@link ExprListenerImpl} is creating a {@link FunctionExpr}, {@link ExprMacroTable} will first be checked to find
 * the function by name, falling back to {@link Parser#getFunction(String)} to map to a built-in {@link Function} if
 * none is defined in the macro table.
 */
public class ExprMacroTable
{
  private static final ExprMacroTable NIL = new ExprMacroTable(Collections.emptyList());

  private final Map<String, ExprMacro> macroMap;

  public ExprMacroTable(final List<ExprMacro> macros)
  {
    this.macroMap = macros.stream().collect(
        Collectors.toMap(
            m -> StringUtils.toLowerCase(m.name()),
            m -> m
        )
    );
  }

  public static ExprMacroTable nil()
  {
    return NIL;
  }

  public List<ExprMacro> getMacros()
  {
    return ImmutableList.copyOf(macroMap.values());
  }

  /**
   * Returns an expr corresponding to a function call if this table has an entry for {@code functionName}.
   * Otherwise, returns null.
   *
   * @param functionName function name
   * @param args         function arguments
   *
   * @return expr for this function call, or null
   */
  @Nullable
  public Expr get(final String functionName, final List<Expr> args)
  {
    final ExprMacro exprMacro = macroMap.get(StringUtils.toLowerCase(functionName));
    if (exprMacro == null) {
      return null;
    }

    return exprMacro.apply(args);
  }

  public interface ExprMacro
  {
    String name();

    Expr apply(List<Expr> args);
  }

  /**
   * Base class for single argument {@link ExprMacro} function {@link Expr}
   */
  public abstract static class BaseScalarUnivariateMacroFunctionExpr implements Expr
  {
    protected final String name;
    protected final Expr arg;

    // Use Supplier to memoize values as ExpressionSelectors#makeExprEvalSelector() can make repeated calls for them
    private final Supplier<BindingDetails> analyzeInputsSupplier;

    public BaseScalarUnivariateMacroFunctionExpr(String name, Expr arg)
    {
      this.name = name;
      this.arg = arg;
      analyzeInputsSupplier = Suppliers.memoize(this::supplyAnalyzeInputs);
    }

    @Override
    public void visit(final Visitor visitor)
    {
      arg.visit(visitor);
      visitor.visit(this);
    }

    @Override
    public BindingDetails analyzeInputs()
    {
      return analyzeInputsSupplier.get();
    }

    @Override
    public String stringify()
    {
      return StringUtils.format("%s(%s)", name, arg.stringify());
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
      BaseScalarUnivariateMacroFunctionExpr that = (BaseScalarUnivariateMacroFunctionExpr) o;
      return Objects.equals(name, that.name) &&
             Objects.equals(arg, that.arg);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(name, arg);
    }

    private BindingDetails supplyAnalyzeInputs()
    {
      return arg.analyzeInputs().withScalarArguments(ImmutableSet.of(arg));
    }
  }

  /**
   * Base class for multi-argument {@link ExprMacro} function {@link Expr}
   */
  public abstract static class BaseScalarMacroFunctionExpr implements Expr
  {
    protected final String name;
    protected final List<Expr> args;

    // Use Supplier to memoize values as ExpressionSelectors#makeExprEvalSelector() can make repeated calls for them
    private final Supplier<BindingDetails> analyzeInputsSupplier;

    public BaseScalarMacroFunctionExpr(String name, final List<Expr> args)
    {
      this.name = name;
      this.args = args;
      analyzeInputsSupplier = Suppliers.memoize(this::supplyAnalyzeInputs);
    }

    @Override
    public String stringify()
    {
      return StringUtils.format(
          "%s(%s)",
          name,
          Expr.ARG_JOINER.join(args.stream().map(Expr::stringify).iterator())
      );
    }

    @Override
    public void visit(final Visitor visitor)
    {
      for (Expr arg : args) {
        arg.visit(visitor);
      }
      visitor.visit(this);
    }

    @Override
    public BindingDetails analyzeInputs()
    {
      return analyzeInputsSupplier.get();
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
      BaseScalarMacroFunctionExpr that = (BaseScalarMacroFunctionExpr) o;
      return Objects.equals(name, that.name) &&
             Objects.equals(args, that.args);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(name, args);
    }

    private BindingDetails supplyAnalyzeInputs()
    {
      final Set<Expr> argSet = Sets.newHashSetWithExpectedSize(args.size());
      BindingDetails accumulator = new BindingDetails();
      for (Expr arg : args) {
        accumulator = accumulator.with(arg);
        argSet.add(arg);
      }
      return accumulator.withScalarArguments(argSet);
    }
  }
}
