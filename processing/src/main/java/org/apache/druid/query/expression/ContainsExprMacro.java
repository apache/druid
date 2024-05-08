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

import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;

import java.util.List;

/**
 * This class implements a function that checks if one string contains another string. It is required that second
 * string be a literal. This expression is case-sensitive.
 * signature:
 * long contains_string(string, string)
 * <p>
 * Examples:
 * - {@code contains_string("foobar", "bar") - 1 }
 * - {@code contains_string("foobar", "car") - 0 }
 * - {@code contains_string("foobar", "Bar") - 0 }
 * <p>
 * @see CaseInsensitiveContainsExprMacro for the case-insensitive version.
 */
public class ContainsExprMacro implements ExprMacroTable.ExprMacro
{
  public static final String FN_NAME = "contains_string";

  @Override
  public String name()
  {
    return FN_NAME;
  }

  @Override
  public Expr apply(final List<Expr> args)
  {
    validationHelperCheckArgumentCount(args, 2);

    final Expr arg = args.get(0);
    final Expr searchStr = args.get(1);
    return new ContainsExpr(this, arg, searchStr, true);
  }
}
