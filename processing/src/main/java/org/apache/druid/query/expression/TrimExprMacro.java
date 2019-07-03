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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;

import javax.annotation.Nonnull;
import java.util.List;

public abstract class TrimExprMacro implements ExprMacroTable.ExprMacro
{
  private static final char[] EMPTY_CHARS = new char[0];
  private static final char[] DEFAULT_CHARS = new char[]{' '};

  enum TrimMode
  {
    BOTH(true, true),
    LEFT(true, false),
    RIGHT(false, true);

    private final boolean left;
    private final boolean right;

    TrimMode(final boolean left, final boolean right)
    {
      this.left = left;
      this.right = right;
    }

    public boolean isLeft()
    {
      return left;
    }

    public boolean isRight()
    {
      return right;
    }
  }

  private final TrimMode mode;
  private final String name;

  public TrimExprMacro(final String name, final TrimMode mode)
  {
    this.name = name;
    this.mode = mode;
  }

  @Override
  public String name()
  {
    return name;
  }

  @Override
  public Expr apply(final List<Expr> args)
  {
    if (args.size() < 1 || args.size() > 2) {
      throw new IAE("Function[%s] must have 1 or 2 arguments", name());
    }

    if (args.size() == 1) {
      return new TrimStaticCharsExpr(mode, args.get(0), DEFAULT_CHARS);
    } else {
      final Expr charsArg = args.get(1);
      if (charsArg.isLiteral()) {
        final String charsString = charsArg.eval(ExprUtils.nilBindings()).asString();
        final char[] chars = charsString == null ? EMPTY_CHARS : charsString.toCharArray();
        return new TrimStaticCharsExpr(mode, args.get(0), chars);
      } else {
        return new TrimDynamicCharsExpr(mode, args.get(0), args.get(1));
      }
    }
  }

  private static class TrimStaticCharsExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
  {
    private final TrimMode mode;
    private final char[] chars;

    public TrimStaticCharsExpr(final TrimMode mode, final Expr stringExpr, final char[] chars)
    {
      super(stringExpr);
      this.mode = mode;
      this.chars = chars;
    }

    @Nonnull
    @Override
    public ExprEval eval(final ObjectBinding bindings)
    {
      final ExprEval stringEval = arg.eval(bindings);

      if (chars.length == 0 || stringEval.value() == null) {
        return stringEval;
      }

      final String s = stringEval.asString();

      int start = 0;
      int end = s.length();

      if (mode.isLeft()) {
        while (start < s.length()) {
          if (arrayContains(chars, s.charAt(start))) {
            start++;
          } else {
            break;
          }
        }
      }

      if (mode.isRight()) {
        while (end > start) {
          if (arrayContains(chars, s.charAt(end - 1))) {
            end--;
          } else {
            break;
          }
        }
      }

      if (start == 0 && end == s.length()) {
        return stringEval;
      } else {
        return ExprEval.of(s.substring(start, end));
      }
    }

    @Override
    public Expr visit(Shuttle shuttle)
    {
      Expr newStringExpr = arg.visit(shuttle);
      return shuttle.visit(new TrimStaticCharsExpr(mode, newStringExpr, chars));
    }
  }

  private static class TrimDynamicCharsExpr implements Expr
  {
    private final TrimMode mode;
    private final Expr stringExpr;
    private final Expr charsExpr;

    public TrimDynamicCharsExpr(final TrimMode mode, final Expr stringExpr, final Expr charsExpr)
    {
      this.mode = mode;
      this.stringExpr = stringExpr;
      this.charsExpr = charsExpr;
    }

    @Nonnull
    @Override
    public ExprEval eval(final ObjectBinding bindings)
    {
      final ExprEval stringEval = stringExpr.eval(bindings);

      if (stringEval.value() == null) {
        return stringEval;
      }

      final ExprEval charsEval = charsExpr.eval(bindings);

      if (charsEval.value() == null) {
        return stringEval;
      }

      final String s = stringEval.asString();
      final String chars = charsEval.asString();

      int start = 0;
      int end = s.length();

      if (mode.isLeft()) {
        while (start < s.length()) {
          if (stringContains(chars, s.charAt(start))) {
            start++;
          } else {
            break;
          }
        }
      }

      if (mode.isRight()) {
        while (end > start) {
          if (stringContains(chars, s.charAt(end - 1))) {
            end--;
          } else {
            break;
          }
        }
      }

      if (start == 0 && end == s.length()) {
        return stringEval;
      } else {
        return ExprEval.of(s.substring(start, end));
      }
    }

    @Override
    public void visit(final Visitor visitor)
    {
      stringExpr.visit(visitor);
      charsExpr.visit(visitor);
      visitor.visit(this);
    }

    @Override
    public Expr visit(Shuttle shuttle)
    {
      Expr newStringExpr = stringExpr.visit(shuttle);
      Expr newCharsExpr = charsExpr.visit(shuttle);
      return shuttle.visit(new TrimDynamicCharsExpr(mode, newStringExpr, newCharsExpr));
    }

    @Override
    public BindingDetails analyzeInputs()
    {
      return stringExpr.analyzeInputs()
                       .with(charsExpr)
                       .withScalarArguments(ImmutableSet.of(stringExpr, charsExpr));
    }
  }

  private static boolean arrayContains(char[] array, char c)
  {
    for (final char arrayChar : array) {
      if (arrayChar == c) {
        return true;
      }
    }

    return false;
  }

  private static boolean stringContains(String string, char c)
  {
    for (int i = 0; i < string.length(); i++) {
      if (string.charAt(i) == c) {
        return true;
      }
    }

    return false;
  }

  public static class BothTrimExprMacro extends TrimExprMacro
  {
    public BothTrimExprMacro()
    {
      super("trim", TrimMode.BOTH);
    }
  }

  public static class LeftTrimExprMacro extends TrimExprMacro
  {
    public LeftTrimExprMacro()
    {
      super("ltrim", TrimMode.LEFT);
    }
  }

  public static class RightTrimExprMacro extends TrimExprMacro
  {
    public RightTrimExprMacro()
    {
      super("rtrim", TrimMode.RIGHT);
    }
  }
}
