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

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.InputBindings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

public abstract class TrimExprMacro implements ExprMacroTable.ExprMacro
{
  private static final char[] EMPTY_CHARS = new char[0];
  private static final char[] DEFAULT_CHARS = new char[]{' '};

  enum TrimMode
  {
    BOTH("trim", true, true),
    LEFT("ltrim", true, false),
    RIGHT("rtrim", false, true);

    private final String name;
    private final boolean left;
    private final boolean right;

    TrimMode(final String name, final boolean left, final boolean right)
    {
      this.name = name;
      this.left = left;
      this.right = right;
    }

    public String getFnName()
    {
      return name;
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

  public TrimExprMacro(final TrimMode mode)
  {
    this.mode = mode;
  }

  @Override
  public String name()
  {
    return mode.getFnName();
  }

  @Override
  public Expr apply(final List<Expr> args)
  {
    validationHelperCheckAnyOfArgumentCount(args, 1, 2);

    if (args.size() == 1) {
      return new TrimStaticCharsExpr(this, args, DEFAULT_CHARS);
    } else {
      final Expr charsArg = args.get(1);
      if (charsArg.isLiteral()) {
        final String charsString = charsArg.eval(InputBindings.nilBindings()).asString();
        final char[] chars = charsString == null ? EMPTY_CHARS : charsString.toCharArray();
        return new TrimStaticCharsExpr(this, args, chars);
      } else {
        return new TrimDynamicCharsExpr(this, args);
      }
    }
  }

  @VisibleForTesting
  static class TrimStaticCharsExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
  {
    private final TrimMode mode;
    private final char[] chars;
    private final Expr stringExpr;

    public TrimStaticCharsExpr(
        final TrimExprMacro macro,
        final List<Expr> args,
        final char[] chars
    )
    {
      super(macro, args);
      this.mode = macro.mode;
      this.stringExpr = args.get(0);
      this.chars = chars;
    }

    @Nonnull
    @Override
    public ExprEval eval(final ObjectBinding bindings)
    {
      final ExprEval stringEval = stringExpr.eval(bindings);

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

    @Nullable
    @Override
    public ExpressionType getOutputType(InputBindingInspector inspector)
    {
      return ExpressionType.STRING;
    }
  }

  @VisibleForTesting
  static class TrimDynamicCharsExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
  {
    private final TrimMode mode;
    private final Expr stringExpr;
    private final Expr charsExpr;

    public TrimDynamicCharsExpr(
        final TrimExprMacro macro,
        final List<Expr> args
    )
    {
      super(macro, args);
      this.mode = macro.mode;
      this.stringExpr = args.get(0);
      this.charsExpr = args.get(1);
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

    @Nullable
    @Override
    public ExpressionType getOutputType(InputBindingInspector inspector)
    {
      return ExpressionType.STRING;
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
      super(TrimMode.BOTH);
    }
  }

  public static class LeftTrimExprMacro extends TrimExprMacro
  {
    public LeftTrimExprMacro()
    {
      super(TrimMode.LEFT);
    }
  }

  public static class RightTrimExprMacro extends TrimExprMacro
  {
    public RightTrimExprMacro()
    {
      super(TrimMode.RIGHT);
    }
  }
}
