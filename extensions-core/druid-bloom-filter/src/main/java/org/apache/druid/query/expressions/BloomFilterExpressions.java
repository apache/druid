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

package org.apache.druid.query.expressions;

import org.apache.druid.guice.BloomFilterSerializersModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.query.aggregation.bloom.BloomFilterAggregatorFactory;
import org.apache.druid.query.filter.BloomKFilter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

public class BloomFilterExpressions
{
  public static final ExpressionType BLOOM_FILTER_TYPE = ExpressionType.fromColumnTypeStrict(
      BloomFilterAggregatorFactory.TYPE
  );

  public static class CreateExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String FN_NAME = "bloom_filter";

    @Override
    public String name()
    {
      return FN_NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      validationHelperCheckArgumentCount(args, 1);

      final Expr expectedSizeArg = args.get(0);

      if (!expectedSizeArg.isLiteral() || expectedSizeArg.getLiteralValue() == null) {
        throw validationFailed("argument must be a LONG constant");
      }

      class BloomExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
      {
        final int expectedSize;

        public BloomExpr(List<Expr> args)
        {
          super(CreateExprMacro.this, args);
          this.expectedSize = args.get(0).eval(InputBindings.nilBindings()).asInt();
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          return ExprEval.ofComplex(
              BLOOM_FILTER_TYPE,
              new BloomKFilter(expectedSize)
          );
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return BLOOM_FILTER_TYPE;
        }
      }

      return new BloomExpr(args);
    }
  }

  public static class AddExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String FN_NAME = "bloom_filter_add";

    @Override
    public String name()
    {
      return FN_NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      validationHelperCheckArgumentCount(args, 2);

      class BloomExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
      {
        private BloomExpr(List<Expr> args)
        {
          super(AddExprMacro.this, args);
        }

        @Override
        public ExprEval eval(final ObjectBinding bindings)
        {
          ExprEval bloomy = args.get(1).eval(bindings);
          // be permissive for now, we can count more on this later when we are better at retaining complete complex
          // type information everywhere
          if (!bloomy.type().equals(BLOOM_FILTER_TYPE) ||
              !bloomy.type().is(ExprType.COMPLEX) && bloomy.value() instanceof BloomKFilter) {
            throw AddExprMacro.this.validationFailed("must take a bloom filter as the second argument");
          }
          BloomKFilter filter = (BloomKFilter) bloomy.value();
          assert filter != null;
          ExprEval input = args.get(0).eval(bindings);

          if (input.value() == null) {
            filter.addBytes(null, 0, 0);
          } else {
            switch (input.type().getType()) {
              case STRING:
                filter.addString(input.asString());
                break;
              case DOUBLE:
                filter.addDouble(input.asDouble());
                break;
              case LONG:
                filter.addLong(input.asLong());
                break;
              case COMPLEX:
                if (BLOOM_FILTER_TYPE.equals(input.type()) ||
                    (bloomy.type().is(ExprType.COMPLEX) && bloomy.value() instanceof BloomKFilter)) {
                  filter.merge((BloomKFilter) input.value());
                  break;
                }
              default:
                throw AddExprMacro.this.validationFailed("cannot add [%s] to a bloom filter", input.type());
            }
          }

          return ExprEval.ofComplex(BLOOM_FILTER_TYPE, filter);
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return BLOOM_FILTER_TYPE;
        }
      }

      return new BloomExpr(args);
    }
  }

  public static class TestExprMacro implements ExprMacroTable.ExprMacro
  {
    public static final String FN_NAME = "bloom_filter_test";

    @Override
    public String name()
    {
      return FN_NAME;
    }

    @Override
    public Expr apply(List<Expr> args)
    {
      validationHelperCheckArgumentCount(args, 2);

      final Expr arg = args.get(0);
      final Expr filterExpr = args.get(1);

      class BloomExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
      {
        private final BloomKFilter filter;

        private BloomExpr(BloomKFilter filter, List<Expr> args)
        {
          super(TestExprMacro.this, args);
          this.filter = filter;
        }

        @Nonnull
        @Override
        public ExprEval eval(final ObjectBinding bindings)
        {
          ExprEval evaluated = arg.eval(bindings);

          boolean matches = false;
          switch (evaluated.type().getType()) {
            case STRING:
              String stringVal = (String) evaluated.value();
              if (stringVal == null) {
                matches = nullMatch();
              } else {
                matches = filter.testString(stringVal);
              }
              break;
            case DOUBLE:
              Double doubleVal = (Double) evaluated.value();
              if (doubleVal == null) {
                matches = nullMatch();
              } else {
                matches = filter.testDouble(doubleVal);
              }
              break;
            case LONG:
              Long longVal = (Long) evaluated.value();
              if (longVal == null) {
                matches = nullMatch();
              } else {
                matches = filter.testLong(longVal);
              }
              break;
          }

          return ExprEval.ofLongBoolean(matches);
        }

        private boolean nullMatch()
        {
          return filter.testBytes(null, 0, 0);
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return ExpressionType.LONG;
        }
      }

      class DynamicBloomExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
      {
        public DynamicBloomExpr(List<Expr> args)
        {
          super(TestExprMacro.this, args);
        }

        @Override
        public ExprEval eval(final ObjectBinding bindings)
        {
          ExprEval bloomy = args.get(1).eval(bindings);
          // be permissive for now, we can count more on this later when we are better at retaining complete complex
          // type information everywhere
          if (!bloomy.type().equals(BLOOM_FILTER_TYPE) ||
              !bloomy.type().is(ExprType.COMPLEX) && bloomy.value() instanceof BloomKFilter) {
            throw TestExprMacro.this.validationFailed("must take a bloom filter as the second argument");
          }
          BloomKFilter filter = (BloomKFilter) bloomy.value();
          assert filter != null;
          ExprEval input = args.get(0).eval(bindings);

          boolean matches = false;
          switch (input.type().getType()) {
            case STRING:
              String stringVal = (String) input.value();
              if (stringVal == null) {
                matches = nullMatch(filter);
              } else {
                matches = filter.testString(stringVal);
              }
              break;
            case DOUBLE:
              Double doubleVal = (Double) input.value();
              if (doubleVal == null) {
                matches = nullMatch(filter);
              } else {
                matches = filter.testDouble(doubleVal);
              }
              break;
            case LONG:
              Long longVal = (Long) input.value();
              if (longVal == null) {
                matches = nullMatch(filter);
              } else {
                matches = filter.testLong(longVal);
              }
              break;
          }

          return ExprEval.ofLongBoolean(matches);
        }

        private boolean nullMatch(BloomKFilter filter)
        {
          return filter.testBytes(null, 0, 0);
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return ExpressionType.LONG;
        }
      }

      if (filterExpr.isLiteral() && filterExpr.getLiteralValue() instanceof String) {
        final String serializedFilter = (String) filterExpr.getLiteralValue();
        final byte[] decoded = StringUtils.decodeBase64String(serializedFilter);
        BloomKFilter filter;
        try {
          filter = BloomFilterSerializersModule.bloomKFilterFromBytes(decoded);
        }
        catch (IOException ioe) {
          throw processingFailed(ioe, "failed to deserialize bloom filter");
        }
        return new BloomExpr(filter, args);
      } else {
        return new DynamicBloomExpr(args);
      }
    }
  }
}
