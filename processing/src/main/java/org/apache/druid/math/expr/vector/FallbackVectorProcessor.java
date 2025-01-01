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

package org.apache.druid.math.expr.vector;

import org.apache.druid.error.DruidException;
import org.apache.druid.math.expr.ApplyFunction;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.Function;
import org.apache.druid.math.expr.LambdaExpr;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Implementation of {@link ExprVectorProcessor} that adapts non-vectorized {@link Expr#eval(Expr.ObjectBinding)}.
 * This allows non-vectorized expressions to participate in vectorized queries.
 */
public abstract class FallbackVectorProcessor<T> implements ExprVectorProcessor<T>
{
  final Supplier<ExprEval<?>> fn;
  final List<AdaptedExpr> adaptedArgs;

  private final ExpressionType outputType;

  private FallbackVectorProcessor(
      final Supplier<ExprEval<?>> fn,
      final List<AdaptedExpr> adaptedArgs,
      final ExpressionType outputType
  )
  {
    this.fn = fn;
    this.adaptedArgs = adaptedArgs;
    this.outputType = outputType;
  }

  /**
   * Create a processor for a non-vectorizable {@link Function}.
   */
  public static <T> FallbackVectorProcessor<T> create(
      final Function function,
      final List<Expr> args,
      final Expr.VectorInputBindingInspector inspector
  )
  {
    final List<Expr> adaptedArgs = makeAdaptedArgs(args, inspector);
    return makeFallbackProcessor(
        () -> function.apply(adaptedArgs, UnusedBinding.INSTANCE),
        adaptedArgs,
        function.getOutputType(inspector, args),
        inspector
    );
  }

  /**
   * Create a processor for a non-vectorizable {@link ApplyFunction}.
   */
  public static <T> FallbackVectorProcessor<T> create(
      final ApplyFunction function,
      final LambdaExpr lambdaExpr,
      final List<Expr> args,
      final Expr.VectorInputBindingInspector inspector
  )
  {
    final List<Expr> adaptedArgs = makeAdaptedArgs(args, inspector);
    return makeFallbackProcessor(
        () -> function.apply(lambdaExpr, adaptedArgs, UnusedBinding.INSTANCE),
        adaptedArgs,
        function.getOutputType(inspector, lambdaExpr, args),
        inspector
    );
  }

  /**
   * Create a processor for a non-vectorizable {@link ExprMacroTable.ExprMacro}.
   */
  public static <T> FallbackVectorProcessor<T> create(
      final ExprMacroTable.ExprMacro macro,
      final List<Expr> args,
      final Expr.VectorInputBindingInspector inspector
  )
  {
    final List<Expr> adaptedArgs = makeAdaptedArgs(args, inspector);
    final Expr adaptedExpr = macro.apply(adaptedArgs);
    return makeFallbackProcessor(
        () -> adaptedExpr.eval(UnusedBinding.INSTANCE),
        adaptedArgs,
        adaptedExpr.getOutputType(inspector),
        inspector
    );
  }

  /**
   * Helper for the two {@link #create} methods. Makes {@link AdaptedExpr} that can replace the original args to
   * the {@link Expr} that requires fallback.
   *
   * @param args      args to the original expr
   * @param inspector binding inspector
   *
   * @return list of {@link AdaptedExpr}
   */
  private static List<Expr> makeAdaptedArgs(
      final List<Expr> args,
      final Expr.VectorInputBindingInspector inspector
  )
  {
    final List<Expr> adaptedArgs = new ArrayList<>(args.size());

    for (final Expr arg : args) {
      adaptedArgs.add(new AdaptedExpr(arg.asVectorProcessor(inspector), arg));
    }

    return adaptedArgs;
  }

  /**
   * Helper for the two {@link #create} methods.
   *
   * @param fn          eval function that uses the "adaptedArgs" as inputs
   * @param adaptedArgs adapted args from {@link #makeAdaptedArgs(List, Expr.VectorInputBindingInspector)}
   * @param outputType  output type of the eval from "fn"
   * @param inspector   binding inspector
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static <T> FallbackVectorProcessor<T> makeFallbackProcessor(
      final Supplier<ExprEval<?>> fn,
      final List<Expr> adaptedArgs,
      final ExpressionType outputType,
      final Expr.VectorInputBindingInspector inspector
  )
  {
    if (outputType == null) {
      throw DruidException.defensive("Plan has null outputType");
    } else if (outputType.equals(ExpressionType.LONG)) {
      return (FallbackVectorProcessor<T>) new OfLong(fn, (List) adaptedArgs, outputType, inspector);
    } else if (outputType.equals(ExpressionType.DOUBLE)) {
      return (FallbackVectorProcessor<T>) new OfDouble(fn, (List) adaptedArgs, outputType, inspector);
    } else {
      return (FallbackVectorProcessor<T>) new OfObject(fn, (List) adaptedArgs, outputType, inspector);
    }
  }

  @Override
  public ExpressionType getOutputType()
  {
    return outputType;
  }

  /**
   * Specialization for non-numeric types.
   */
  private static class OfObject extends FallbackVectorProcessor<Object[]>
  {
    private final Object[] outValues;

    public OfObject(
        final Supplier<ExprEval<?>> fn,
        final List<AdaptedExpr> args,
        final ExpressionType outputType,
        final Expr.VectorInputBindingInspector inspector
    )
    {
      super(fn, args, outputType);
      this.outValues = new Object[inspector.getMaxVectorSize()];
    }

    @Override
    public ExprEvalVector<Object[]> evalVector(Expr.VectorInputBinding vectorBindings)
    {
      for (final AdaptedExpr adaptedArg : adaptedArgs) {
        adaptedArg.populate(vectorBindings);
      }

      final int sz = vectorBindings.getCurrentVectorSize();
      for (int i = 0; i < sz; i++) {
        for (final AdaptedExpr adaptedArg : adaptedArgs) {
          adaptedArg.setRowNumber(i);
        }

        outValues[i] = fn.get().value();
      }

      return new ExprEvalObjectVector(outValues, getOutputType());
    }
  }

  /**
   * Specialization for {@link ExpressionType#LONG}.
   */
  private static class OfLong extends FallbackVectorProcessor<long[]>
  {
    private final long[] outValues;
    private final boolean[] outNulls;

    public OfLong(
        final Supplier<ExprEval<?>> fn,
        final List<AdaptedExpr> args,
        final ExpressionType outputType,
        final Expr.VectorInputBindingInspector inspector
    )
    {
      super(fn, args, outputType);
      this.outValues = new long[inspector.getMaxVectorSize()];
      this.outNulls = new boolean[inspector.getMaxVectorSize()];
    }

    @Override
    public ExprEvalVector<long[]> evalVector(Expr.VectorInputBinding vectorBindings)
    {
      for (final AdaptedExpr adaptedArg : adaptedArgs) {
        adaptedArg.populate(vectorBindings);
      }

      final int sz = vectorBindings.getCurrentVectorSize();
      boolean anyNulls = false;

      for (int i = 0; i < sz; i++) {
        for (final AdaptedExpr adaptedArg : adaptedArgs) {
          adaptedArg.setRowNumber(i);
        }

        final ExprEval<?> eval = fn.get();
        outValues[i] = eval.asLong();
        outNulls[i] = eval.isNumericNull();
        anyNulls = anyNulls || outNulls[i];
      }

      return new ExprEvalLongVector(outValues, anyNulls ? outNulls : null);
    }
  }

  /**
   * Specialization for {@link ExpressionType#DOUBLE}.
   */
  private static class OfDouble extends FallbackVectorProcessor<double[]>
  {
    private final double[] outValues;
    private final boolean[] outNulls;

    public OfDouble(
        final Supplier<ExprEval<?>> fn,
        final List<AdaptedExpr> args,
        final ExpressionType outputType,
        final Expr.VectorInputBindingInspector inspector
    )
    {
      super(fn, args, outputType);
      this.outValues = new double[inspector.getMaxVectorSize()];
      this.outNulls = new boolean[inspector.getMaxVectorSize()];
    }

    @Override
    public ExprEvalVector<double[]> evalVector(Expr.VectorInputBinding vectorBindings)
    {
      for (final AdaptedExpr adaptedArg : adaptedArgs) {
        adaptedArg.populate(vectorBindings);
      }

      final int sz = vectorBindings.getCurrentVectorSize();
      boolean anyNulls = false;

      for (int i = 0; i < sz; i++) {
        for (final AdaptedExpr adaptedArg : adaptedArgs) {
          adaptedArg.setRowNumber(i);
        }

        final ExprEval<?> eval = fn.get();
        outValues[i] = eval.asDouble();
        outNulls[i] = eval.isNumericNull();
        anyNulls = anyNulls || outNulls[i];
      }

      return new ExprEvalDoubleVector(outValues, anyNulls ? outNulls : null);
    }
  }

  /**
   * Wrapper around {@link Expr} that pulls results from a {@link ExprVectorProcessor} rather than calling
   * {@link Expr#eval(ObjectBinding)}. When using {@link FallbackVectorProcessor}, adapters of this class replace
   * the arguments of the original {@link Expr}.
   */
  private static class AdaptedExpr implements Expr
  {
    private final ExprVectorProcessor<?> processor;
    private final Expr originalExpr;
    private final ExpressionType type;

    private ExprEvalVector<?> results;
    private int rowNum;

    public AdaptedExpr(final ExprVectorProcessor<?> processor, final Expr originalExpr)
    {
      this.processor = processor;
      this.originalExpr = originalExpr;
      this.type = processor.getOutputType();
    }

    /**
     * Populate the {@link #results} vector. Called once per vector.
     */
    public void populate(final Expr.VectorInputBinding vectorBindings)
    {
      this.results = processor.evalVector(vectorBindings);
    }

    /**
     * Set {@link #rowNum}, which controls which row of {@link #results} is returned by {@link #eval(ObjectBinding)}.
     */
    public void setRowNumber(final int rowNum)
    {
      this.rowNum = rowNum;
    }

    @Override
    public ExprEval eval(ObjectBinding bindings)
    {
      if (results == null) {
        // "results" can be null if eval is called during ExprMacro#apply.
        return originalExpr.eval(bindings);
      }

      // In all other cases, ignore the provided bindings and use the computed "results" instead.
      if (type.is(ExprType.LONG)) {
        final boolean isNull = results.getNullVector() != null && results.getNullVector()[rowNum];
        return ExprEval.ofLong(isNull ? null : results.getLongVector()[rowNum]);
      } else if (type.is(ExprType.DOUBLE)) {
        final boolean isNull = results.getNullVector() != null && results.getNullVector()[rowNum];
        return ExprEval.ofDouble(isNull ? null : results.getDoubleVector()[rowNum]);
      } else {
        return ExprEval.ofType(type, results.getObjectVector()[rowNum]);
      }
    }

    @Override
    public String stringify()
    {
      throw DruidException.defensive(
          "Unexpected call to stringify in fallback processor for expr[%s]",
          originalExpr.stringify()
      );
    }

    @Override
    public Expr visit(Shuttle shuttle)
    {
      throw DruidException.defensive(
          "Unexpected call to visit in fallback processor for expr[%s]",
          originalExpr.stringify()
      );
    }

    @Override
    public BindingAnalysis analyzeInputs()
    {
      return originalExpr.analyzeInputs();
    }

    @Override
    public boolean isLiteral()
    {
      return originalExpr.isLiteral();
    }

    @Override
    public boolean isNullLiteral()
    {
      return originalExpr.isNullLiteral();
    }

    @Nullable
    @Override
    public Object getLiteralValue()
    {
      return originalExpr.getLiteralValue();
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
      AdaptedExpr that = (AdaptedExpr) o;
      return Objects.equals(originalExpr, that.originalExpr) && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(originalExpr, type);
    }
  }

  /**
   * Implementation of {@link Expr.ObjectBinding} where we do not actually expect any methods to be called. This is
   * because bindings should only be used by identifiers, and this fallback processor is never used to
   * implement identifiers.
   */
  private static class UnusedBinding implements Expr.ObjectBinding
  {
    public static final UnusedBinding INSTANCE = new UnusedBinding();

    @Nullable
    @Override
    public Object get(String name)
    {
      throw DruidException.defensive("Unexpected binding.get call for field[%s]", name);
    }

    @Nullable
    @Override
    public ExpressionType getType(String name)
    {
      throw DruidException.defensive("Unexpected binding.getType call for field[%s]", name);
    }
  }
}
