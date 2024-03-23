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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.aggregation.cardinality.CardinalityAggregator;
import org.apache.druid.query.aggregation.cardinality.types.StringCardinalityAggregatorColumnSelectorStrategy;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class HyperUniqueExpressions
{
  public static final ExpressionType TYPE = ExpressionType.fromColumnType(HyperUniquesAggregatorFactory.TYPE);

  public static class HllCreateExprMacro implements ExprMacroTable.ExprMacro
  {
    private static final String NAME = "hyper_unique";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      validationHelperCheckArgumentCount(args, 0);
      final HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();
      class HllExpression implements ExprMacroTable.ExprMacroFunctionExpr
      {
        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          return ExprEval.ofComplex(TYPE, collector);
        }

        @Override
        public String stringify()
        {
          return StringUtils.format("%s()", NAME);
        }

        @Override
        public Expr visit(Shuttle shuttle)
        {
          return shuttle.visit(this);
        }

        @Override
        public BindingAnalysis analyzeInputs()
        {
          return BindingAnalysis.EMTPY;
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return TYPE;
        }

        @Override
        public List<Expr> getArgs()
        {
          return Collections.emptyList();
        }

        @Override
        public int hashCode()
        {
          return Objects.hashCode(NAME);
        }

        @Override
        public boolean equals(Object obj)
        {
          if (this == obj) {
            return true;
          }
          if (obj == null || getClass() != obj.getClass()) {
            return false;
          }
          return true;
        }

        @Override
        public String toString()
        {
          return StringUtils.format("(%s)", NAME);
        }
      }
      return new HllExpression();
    }
  }

  public static class HllAddExprMacro implements ExprMacroTable.ExprMacro
  {
    private static final String NAME = "hyper_unique_add";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      validationHelperCheckArgumentCount(args, 2);

      class HllExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
      {
        public HllExpr(List<Expr> args)
        {
          super(HllAddExprMacro.this, args);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          ExprEval hllCollector = args.get(1).eval(bindings);
          ExpressionType hllType = hllCollector.type();
          // be permissive for now, we can count more on this later when we are better at retaining complete complex
          // type information everywhere
          if (!TYPE.equals(hllType) ||
              !(hllType.is(ExprType.COMPLEX) && hllCollector.value() instanceof HyperLogLogCollector)
          ) {
            throw HllAddExprMacro.this.validationFailed(
                "requires a hyper-log-log collector as the second argument"
            );
          }
          HyperLogLogCollector collector = (HyperLogLogCollector) hllCollector.value();
          assert collector != null;
          ExprEval input = args.get(0).eval(bindings);
          switch (input.type().getType()) {
            case STRING:
              if (input.value() == null) {
                if (NullHandling.replaceWithDefault()) {
                  collector.add(
                      CardinalityAggregator.HASH_FUNCTION.hashUnencodedChars(
                          StringCardinalityAggregatorColumnSelectorStrategy.CARDINALITY_AGG_NULL_STRING
                      ).asBytes()
                  );
                }
              } else {
                collector.add(CardinalityAggregator.HASH_FUNCTION.hashUnencodedChars(input.asString()).asBytes());
              }
              break;
            case DOUBLE:
              if (NullHandling.replaceWithDefault() || !input.isNumericNull()) {
                collector.add(CardinalityAggregator.HASH_FUNCTION.hashLong(Double.doubleToLongBits(input.asDouble()))
                                                                 .asBytes());
              }
              break;
            case LONG:
              if (NullHandling.replaceWithDefault() || !input.isNumericNull()) {
                collector.add(CardinalityAggregator.HASH_FUNCTION.hashLong(input.asLong()).asBytes());
              }
              break;
            case COMPLEX:
              if (TYPE.equals(input.type())
                  || hllType.is(ExprType.COMPLEX) && hllCollector.value() instanceof HyperLogLogCollector) {
                collector.fold((HyperLogLogCollector) input.value());
                break;
              }
            default:
              throw HllAddExprMacro.this.validationFailed(
                  "cannot add [%s] to hyper-log-log collector",
                  input.type()
              );
          }

          return ExprEval.ofComplex(TYPE, collector);
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return TYPE;
        }
      }
      return new HllExpr(args);
    }
  }

  public static class HllEstimateExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "hyper_unique_estimate";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      validationHelperCheckArgumentCount(args, 1);

      class HllExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
      {
        public HllExpr(List<Expr> args)
        {
          super(HllEstimateExprMacro.this, args);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          ExprEval hllCollector = args.get(0).eval(bindings);
          // be permissive for now, we can count more on this later when we are better at retaining complete complex
          // type information everywhere
          if (!TYPE.equals(hllCollector.type()) ||
              !(hllCollector.type().is(ExprType.COMPLEX) && hllCollector.value() instanceof HyperLogLogCollector)
          ) {
            throw HllEstimateExprMacro.this.validationFailed(
                "requires a hyper-log-log collector as input but got %s instead",
                hllCollector.type()
            );
          }
          HyperLogLogCollector collector = (HyperLogLogCollector) hllCollector.value();
          assert collector != null;
          return ExprEval.ofDouble(collector.estimateCardinality());
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return ExpressionType.DOUBLE;
        }
      }
      return new HllExpr(args);
    }
  }

  public static class HllRoundEstimateExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String NAME = "hyper_unique_round_estimate";

    @Override
    public String name()
    {
      return NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      validationHelperCheckArgumentCount(args, 1);

      class HllExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
      {
        public HllExpr(List<Expr> args)
        {
          super(HllRoundEstimateExprMacro.this, args);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          ExprEval hllCollector = args.get(0).eval(bindings);
          if (!hllCollector.type().equals(TYPE)) {
            throw HllRoundEstimateExprMacro.this.validationFailed(
                "requires a hyper-log-log collector as input but got %s instead",
                hllCollector.type()
            );
          }
          HyperLogLogCollector collector = (HyperLogLogCollector) hllCollector.value();
          assert collector != null;
          return ExprEval.ofLong(collector.estimateCardinalityRound());
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return ExpressionType.LONG;
        }
      }
      return new HllExpr(args);
    }
  }
}
