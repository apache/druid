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

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;

import javax.annotation.Nonnull;
import java.util.List;

/**
 *  hash('hash_func_name', expr, expr ...) outputs hash of given expressions using given hash
 *  function name. Currently, only "MD5" is supported, will output 128 bits in hex format,
 *  e.g., "db2851326a0706e5fe0c444243358cbe"
 */
public class HashExprMacro implements ExprMacroTable.ExprMacro
{
  // Supported hash functions
  enum HASH_FUNCS
  {
    MD5
  }

  @Override
  public String name()
  {
    return "hash";
  }

  @Override
  public Expr apply(final List<Expr> args)
  {
    if (args.size() < 2) {
      throw new IAE("Function[%s] must have at least 2 argument", name());
    }

    final Expr hashFuncNameArg = args.get(0);
    if (!hashFuncNameArg.isLiteral()) {
      throw new IAE("Function[%s] hash_func_name must be literals", name());
    }

    String hashFuncName = String.valueOf(hashFuncNameArg.getLiteralValue());
    final HASH_FUNCS hashFunc;
    try {
      hashFunc = HASH_FUNCS.valueOf(hashFuncName);
    }
    catch (IllegalArgumentException e) {
      throw new IAE("Function[%s] hash_func_name [%s] is not supported", name(),
          hashFuncName);
    }

    class HashExtractExpr implements Expr
    {
      @Nonnull
      @Override
      public ExprEval eval(final ObjectBinding bindings)
      {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i < args.size(); i++) {
          Expr arg = args.get(i);
          String s = arg.eval(bindings).asString();
          sb.append(s);
        }

        String retVal;
        if (hashFunc.equals(HASH_FUNCS.MD5)) {
          retVal = DigestUtils.md5Hex(sb.toString());
        } else {
          throw new IAE("Function[%s] hash_func_name [%s] is not supported",
              hashFuncName);
        }

        return ExprEval.of(NullHandling.emptyToNullIfNeeded(retVal));
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
      public Expr visit(final Expr.Shuttle shuttle)
      {
        return shuttle.visit(this);
      }

      @Override
      public BindingDetails analyzeInputs()
      {
        return new BindingDetails();
      }
    }

    return new HashExtractExpr();
  }
}
