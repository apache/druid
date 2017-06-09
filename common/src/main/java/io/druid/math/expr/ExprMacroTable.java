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

import io.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    Expr apply(final List<Expr> args);
  }
}
