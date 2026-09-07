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

package org.apache.druid.query.expression;

import com.google.inject.Inject;
import org.apache.druid.math.expr.Evals;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.vector.CastToTypeVectorProcessor;
import org.apache.druid.math.expr.vector.ExprEvalObjectVector;
import org.apache.druid.math.expr.vector.ExprEvalVector;
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
import org.apache.druid.math.expr.vector.UnivariateObjectFunctionVectorProcessor;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.lookup.RegisteredLookupExtractionFn;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

public class LookupExprMacro implements ExprMacroTable.ExprMacro
{
  private static final String FN_NAME = "lookup";
  private final LookupExtractorFactoryContainerProvider lookupExtractorFactoryContainerProvider;

  @Inject
  public LookupExprMacro(final LookupExtractorFactoryContainerProvider lookupExtractorFactoryContainerProvider)
  {
    this.lookupExtractorFactoryContainerProvider = lookupExtractorFactoryContainerProvider;
  }

  @Override
  public String name()
  {
    return FN_NAME;
  }

  @Override
  public Expr apply(final List<Expr> args)
  {
    validationHelperCheckArgumentRange(args, 2, 3);

    final Expr arg = args.get(0);
    final Expr lookupExpr = args.get(1);
    final Expr replaceMissingValueWith = getReplaceMissingValueWith(args);

    validationHelperCheckArgIsLiteral(lookupExpr, "second argument");
    if (lookupExpr.getLiteralValue() == null) {
      throw validationFailed("second argument must be a registered lookup name");
    }

    final String lookupName = lookupExpr.getLiteralValue().toString();
    final RegisteredLookupExtractionFn extractionFn = new RegisteredLookupExtractionFn(
        lookupExtractorFactoryContainerProvider,
        lookupName,
        false,
        replaceMissingValueWith != null && replaceMissingValueWith.isLiteral()
        ? Evals.asString(replaceMissingValueWith.getLiteralValue())
        : null,
        null,
        null
    );

    class LookupExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
    {
      private LookupExpr(final List<Expr> args)
      {
        super(LookupExprMacro.this, args);
      }

      @Nonnull
      @Override
      public ExprEval eval(final ObjectBinding bindings)
      {
        return ExprEval.ofString(extractionFn.apply(arg.eval(bindings).asString()));
      }

      @Nullable
      @Override
      public ExpressionType getOutputType(InputBindingInspector inspector)
      {
        return ExpressionType.STRING;
      }
      @Override
      public boolean canVectorize(InputBindingInspector inspector)
      {
        return true;
      }

      @Override
      public <T> ExprVectorProcessor<T> asVectorProcessor(VectorInputBindingInspector inspector)
      {
        final Object[] outputs = new Object[inspector.getMaxVectorSize()];
        final ExprVectorProcessor<Object[]> processor =
            new UnivariateObjectFunctionVectorProcessor<Object[], Object[]>(
                CastToTypeVectorProcessor.cast(arg.asVectorProcessor(inspector), ExpressionType.STRING),
                outputs
            )
            {
              @Override
              public ExpressionType getOutputType()
              {
                return ExpressionType.STRING;
              }
              @Override
              public int maxVectorSize()
              {
                return inspector.getMaxVectorSize();
              }
              @Override
              public void processIndex(Object[] input, Object[] output, boolean[] outputNulls, int i)
              {
                final String extracted = extractionFn.apply(input[i]);
                outputs[i] = extracted;
              }
              @Override
              public ExprEvalVector asEval()
              {
                return new ExprEvalObjectVector(outputs, ExpressionType.STRING);
              }
            };
        return (ExprVectorProcessor<T>) processor;
      }

      @Override
      public void decorateCacheKeyBuilder(CacheKeyBuilder builder)
      {
        builder.appendCacheable(extractionFn);
      }
    }

    return new LookupExpr(args);
  }

  private Expr getReplaceMissingValueWith(final List<Expr> args)
  {
    if (args.size() > 2) {
      final Expr missingValExpr = args.get(2);
      validationHelperCheckArgIsLiteral(missingValExpr, "third argument");
      return missingValExpr;
    }
    return null;
  }
}
