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

import com.google.common.collect.ImmutableSet;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public abstract class MacroTestBase extends InitializedNullHandlingTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final ExprMacroTable.ExprMacro macro;

  protected MacroTestBase(ExprMacroTable.ExprMacro macro)
  {
    this.macro = macro;
  }

  protected void expectException(Class<? extends Throwable> type, String message)
  {
    expectedException.expect(type);
    expectedException.expectMessage(message);
  }

  protected Expr apply(final List<Expr> args)
  {
    return macro.apply(args);
  }

  /**
   * Evalutes {@code expr} using our macro.
   *
   * @param expression expression to evalute
   * @param bindings   bindings for evaluation
   *
   * @throws AssertionError if {@link ExprMacroTable.ExprMacro#apply} is not called on our macro during parsing
   */
  protected ExprEval<?> eval(
      final String expression,
      final Expr.ObjectBinding bindings
  )
  {
    // WrappedExprMacro allows us to confirm that our ExprMacro was actually called.
    class WrappedExprMacro implements ExprMacroTable.ExprMacro
    {
      private final AtomicLong calls = new AtomicLong();

      @Override
      public String name()
      {
        return macro.name();
      }

      @Override
      public Expr apply(List<Expr> args)
      {
        calls.incrementAndGet();
        return macro.apply(args);
      }
    }

    final WrappedExprMacro wrappedMacro = new WrappedExprMacro();
    final GuiceExprMacroTable macroTable = new GuiceExprMacroTable(ImmutableSet.of(wrappedMacro));
    final Expr expr = Parser.parse(expression, macroTable);

    Assert.assertTrue("Calls made to macro.apply", wrappedMacro.calls.get() > 0);

    return expr.eval(bindings);
  }
}
