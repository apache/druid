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
import com.google.common.collect.Maps;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Mechanism by which Druid expressions can define new functions for the Druid expression language. When
 * {@link ExprListenerImpl} is creating a {@link FunctionExpr}, {@link ExprMacroTable} will first be checked to find
 * the function by name, falling back to {@link Parser#getFunction(String)} to map to a built-in {@link Function} if
 * none is defined in the macro table.
 */
public class ExprMacroTable
{
  private static final BuiltInExprMacros.ComplexDecodeBase64ExprMacro COMPLEX_DECODE_BASE_64_EXPR_MACRO = new BuiltInExprMacros.ComplexDecodeBase64ExprMacro();
  private static final List<ExprMacro> BUILT_IN = ImmutableList.of(
      COMPLEX_DECODE_BASE_64_EXPR_MACRO,
      new AliasExprMacro(
          COMPLEX_DECODE_BASE_64_EXPR_MACRO,
          BuiltInExprMacros.ComplexDecodeBase64ExprMacro.ALIAS
      ),
      new BuiltInExprMacros.StringDecodeBase64UTFExprMacro()
  );
  private static final ExprMacroTable NIL = new ExprMacroTable(Collections.emptyList());

  private final Map<String, ExprMacro> macroMap;

  public ExprMacroTable(final List<ExprMacro> macros)
  {
    this.macroMap = Maps.newHashMapWithExpectedSize(BUILT_IN.size() + macros.size());
    macroMap.putAll(BUILT_IN.stream().collect(Collectors.toMap(m -> StringUtils.toLowerCase(m.name()), m -> m)));
    macroMap.putAll(macros.stream().collect(Collectors.toMap(m -> StringUtils.toLowerCase(m.name()), m -> m)));
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

  public interface ExprMacro extends NamedFunction
  {
    Expr apply(List<Expr> args);
  }

  /**
   * stub interface to allow {@link Parser#flatten(Expr)} a way to recognize macro functions that exend this
   */
  public interface ExprMacroFunctionExpr extends Expr
  {
    List<Expr> getArgs();
  }

  /**
   * Base class for {@link Expr} from {@link ExprMacro}.
   */
  public abstract static class BaseMacroFunctionExpr implements ExprMacroFunctionExpr
  {
    protected final ExprMacro macro;
    protected final List<Expr> args;

    // Use Supplier to memoize values as ExpressionSelectors#makeExprEvalSelector() can make repeated calls for them
    private final Supplier<BindingAnalysis> analyzeInputsSupplier;

    /**
     * Constructor for subclasses.
     *
     * @param macro     macro that created this expr
     * @param macroArgs original args to the macro (not the ones this will be evaled with)
     */
    protected BaseMacroFunctionExpr(final ExprMacro macro, final List<Expr> macroArgs)
    {
      this.macro = macro;
      this.args = macroArgs;
      analyzeInputsSupplier = Suppliers.memoize(this::supplyAnalyzeInputs);
    }

    @Override
    public List<Expr> getArgs()
    {
      return args;
    }

    @Override
    public String stringify()
    {
      return StringUtils.format(
          "%s(%s)",
          macro.name(),
          args.size() == 1
          ? args.get(0).stringify()
          : Expr.ARG_JOINER.join(args.stream().map(Expr::stringify).iterator())
      );
    }

    @Override
    public Expr visit(Shuttle shuttle)
    {
      return shuttle.visit(macro.apply(shuttle.visitAll(args)));
    }

    @Override
    public BindingAnalysis analyzeInputs()
    {
      return analyzeInputsSupplier.get();
    }

    /**
     * Implemented by subclasses to provide the value for {@link #analyzeInputs()}, which uses a memoized supplier.
     */
    protected abstract BindingAnalysis supplyAnalyzeInputs();

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
      return Objects.equals(macro, that.macro) &&
             Objects.equals(args, that.args);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(macro, args);
    }

    @Override
    public String toString()
    {
      return StringUtils.format("(%s %s)", macro.name(), getArgs());
    }
  }

  /**
   * Base class for {@link Expr} from {@link ExprMacro} that accepts all-scalar arguments.
   */
  public abstract static class BaseScalarMacroFunctionExpr extends BaseMacroFunctionExpr
  {
    public BaseScalarMacroFunctionExpr(final ExprMacro macro, final List<Expr> macroArgs)
    {
      super(macro, macroArgs);
    }

    @Override
    protected BindingAnalysis supplyAnalyzeInputs()
    {
      return Exprs.analyzeBindings(args)
                  .withScalarArguments(ImmutableSet.copyOf(args));
    }
  }

  /***
   * Alias Expression macro to create an alias and delegate operations to the same base macro.
   * The Expr spit out by the apply method should use name() in all the places instead of an internal constant so that things like error messages behave correctly.
   */
  static class AliasExprMacro implements ExprMacroTable.ExprMacro
  {
    private final ExprMacroTable.ExprMacro exprMacro;
    private final String alias;

    public AliasExprMacro(final ExprMacroTable.ExprMacro baseExprMacro, final String alias)
    {
      this.exprMacro = baseExprMacro;
      this.alias = alias;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      return exprMacro.apply(args);
    }

    @Override
    public String name()
    {
      return alias;
    }
  }
}
