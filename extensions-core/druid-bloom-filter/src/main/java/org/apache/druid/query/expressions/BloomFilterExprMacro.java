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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.query.filter.BloomKFilter;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;

public class BloomFilterExprMacro implements ExprMacroTable.ExprMacro
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
    if (args.size() != 2) {
      throw new IAE("Function[%s] must have 2 arguments", name());
    }

    final Expr arg = args.get(0);
    final Expr filterExpr = args.get(1);

    if (!filterExpr.isLiteral() || filterExpr.getLiteralValue() == null) {
      throw new IAE("Function[%s] second argument must be a base64 serialized bloom filter", name());
    }


    final String serializedFilter = filterExpr.getLiteralValue().toString();
    final byte[] decoded = StringUtils.decodeBase64String(serializedFilter);
    BloomKFilter filter;
    try {
      filter = BloomFilterSerializersModule.bloomKFilterFromBytes(decoded);
    }
    catch (IOException ioe) {
      throw new RuntimeException("Failed to deserialize bloom filter", ioe);
    }

    class BloomExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
    {
      private BloomExpr(Expr arg)
      {
        super(FN_NAME, arg);
      }

      @Nonnull
      @Override
      public ExprEval eval(final ObjectBinding bindings)
      {
        ExprEval evaluated = arg.eval(bindings);

        boolean matches = false;
        switch (evaluated.type()) {
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

        return ExprEval.of(matches, ExprType.LONG);
      }

      private boolean nullMatch()
      {
        return filter.testBytes(null, 0, 0);
      }


      @Override
      public Expr visit(Shuttle shuttle)
      {
        Expr newArg = arg.visit(shuttle);
        return shuttle.visit(new BloomExpr(newArg));
      }
    }

    return new BloomExpr(arg);
  }
}
