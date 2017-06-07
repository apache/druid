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

import io.druid.java.util.common.RE;
import io.druid.math.expr.antlr.ExprBaseListener;
import io.druid.math.expr.antlr.ExprParser;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang.StringEscapeUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class ExprListenerImpl extends ExprBaseListener
{
  private final Map<ParseTree, Object> nodes;
  private final ExprMacroTable macroTable;
  private final ParseTree rootNodeKey;

  ExprListenerImpl(ParseTree rootNodeKey, ExprMacroTable macroTable)
  {
    this.rootNodeKey = rootNodeKey;
    this.macroTable = macroTable;
    this.nodes = new HashMap<>();
  }

  Expr getAST()
  {
    return (Expr) nodes.get(rootNodeKey);
  }

  @Override
  public void exitUnaryOpExpr(ExprParser.UnaryOpExprContext ctx)
  {
    int opCode = ((TerminalNode) ctx.getChild(0)).getSymbol().getType();
    switch (opCode) {
      case ExprParser.MINUS:
        nodes.put(ctx, new UnaryMinusExpr((Expr) nodes.get(ctx.getChild(1))));
        break;
      case ExprParser.NOT:
        nodes.put(ctx, new UnaryNotExpr((Expr) nodes.get(ctx.getChild(1))));
        break;
      default:
        throw new RuntimeException("Unrecognized unary operator " + ctx.getChild(0).getText());
    }
  }

  @Override
  public void exitDoubleExpr(ExprParser.DoubleExprContext ctx)
  {
    nodes.put(
        ctx,
        new DoubleExpr(Double.parseDouble(ctx.getText()))
    );
  }

  @Override
  public void exitAddSubExpr(ExprParser.AddSubExprContext ctx)
  {
    int opCode = ((TerminalNode) ctx.getChild(1)).getSymbol().getType();
    switch (opCode) {
      case ExprParser.PLUS:
        nodes.put(
            ctx,
            new BinPlusExpr(
                ctx.getChild(1).getText(),
                (Expr) nodes.get(ctx.getChild(0)),
                (Expr) nodes.get(ctx.getChild(2))
            )
        );
        break;
      case ExprParser.MINUS:
        nodes.put(
            ctx,
            new BinMinusExpr(
                ctx.getChild(1).getText(),
                (Expr) nodes.get(ctx.getChild(0)),
                (Expr) nodes.get(ctx.getChild(2))
            )
        );
        break;
      default:
        throw new RuntimeException("Unrecognized binary operator " + ctx.getChild(1).getText());
    }
  }

  @Override
  public void exitLongExpr(ExprParser.LongExprContext ctx)
  {
    nodes.put(
        ctx,
        new LongExpr(Long.parseLong(ctx.getText()))
    );
  }

  @Override
  public void exitLogicalAndOrExpr(ExprParser.LogicalAndOrExprContext ctx)
  {
    int opCode = ((TerminalNode) ctx.getChild(1)).getSymbol().getType();
    switch (opCode) {
      case ExprParser.AND:
        nodes.put(
            ctx,
            new BinAndExpr(
                ctx.getChild(1).getText(),
                (Expr) nodes.get(ctx.getChild(0)),
                (Expr) nodes.get(ctx.getChild(2))
            )
        );
        break;
      case ExprParser.OR:
        nodes.put(
            ctx,
            new BinOrExpr(
                ctx.getChild(1).getText(),
                (Expr) nodes.get(ctx.getChild(0)),
                (Expr) nodes.get(ctx.getChild(2))
            )
        );
        break;
      default:
        throw new RuntimeException("Unrecognized binary operator " + ctx.getChild(1).getText());
    }
  }

  @Override
  public void exitNestedExpr(ExprParser.NestedExprContext ctx)
  {
    nodes.put(ctx, nodes.get(ctx.getChild(1)));
  }

  @Override
  public void exitString(ExprParser.StringContext ctx)
  {
    String text = ctx.getText();
    String unquoted = text.substring(1, text.length() - 1);
    String unescaped = unquoted.indexOf('\\') >= 0 ? StringEscapeUtils.unescapeJava(unquoted) : unquoted;
    nodes.put(ctx, new StringExpr(unescaped));
  }

  @Override
  public void exitLogicalOpExpr(ExprParser.LogicalOpExprContext ctx)
  {
    int opCode = ((TerminalNode) ctx.getChild(1)).getSymbol().getType();
    switch (opCode) {
      case ExprParser.LT:
        nodes.put(
            ctx,
            new BinLtExpr(
                ctx.getChild(1).getText(),
                (Expr) nodes.get(ctx.getChild(0)),
                (Expr) nodes.get(ctx.getChild(2))
            )
        );
        break;
      case ExprParser.LEQ:
        nodes.put(
            ctx,
            new BinLeqExpr(
                ctx.getChild(1).getText(),
                (Expr) nodes.get(ctx.getChild(0)),
                (Expr) nodes.get(ctx.getChild(2))
            )
        );
        break;
      case ExprParser.GT:
        nodes.put(
            ctx,
            new BinGtExpr(
                ctx.getChild(1).getText(),
                (Expr) nodes.get(ctx.getChild(0)),
                (Expr) nodes.get(ctx.getChild(2))
            )
        );
        break;
      case ExprParser.GEQ:
        nodes.put(
            ctx,
            new BinGeqExpr(
                ctx.getChild(1).getText(),
                (Expr) nodes.get(ctx.getChild(0)),
                (Expr) nodes.get(ctx.getChild(2))
            )
        );
        break;
      case ExprParser.EQ:
        nodes.put(
            ctx,
            new BinEqExpr(
                ctx.getChild(1).getText(),
                (Expr) nodes.get(ctx.getChild(0)),
                (Expr) nodes.get(ctx.getChild(2))
            )
        );
        break;
      case ExprParser.NEQ:
        nodes.put(
            ctx,
            new BinNeqExpr(
                ctx.getChild(1).getText(),
                (Expr) nodes.get(ctx.getChild(0)),
                (Expr) nodes.get(ctx.getChild(2))
            )
        );
        break;
      default:
        throw new RuntimeException("Unrecognized binary operator " + ctx.getChild(1).getText());
    }
  }

  @Override
  public void exitMulDivModuloExpr(ExprParser.MulDivModuloExprContext ctx)
  {
    int opCode = ((TerminalNode) ctx.getChild(1)).getSymbol().getType();
    switch (opCode) {
      case ExprParser.MUL:
        nodes.put(
            ctx,
            new BinMulExpr(
                ctx.getChild(1).getText(),
                (Expr) nodes.get(ctx.getChild(0)),
                (Expr) nodes.get(ctx.getChild(2))
            )
        );
        break;
      case ExprParser.DIV:
        nodes.put(
            ctx,
            new BinDivExpr(
                ctx.getChild(1).getText(),
                (Expr) nodes.get(ctx.getChild(0)),
                (Expr) nodes.get(ctx.getChild(2))
            )
        );
        break;
      case ExprParser.MODULO:
        nodes.put(
            ctx,
            new BinModuloExpr(
                ctx.getChild(1).getText(),
                (Expr) nodes.get(ctx.getChild(0)),
                (Expr) nodes.get(ctx.getChild(2))
            )
        );
        break;
      default:
        throw new RuntimeException("Unrecognized binary operator " + ctx.getChild(1).getText());
    }
  }

  @Override
  public void exitPowOpExpr(ExprParser.PowOpExprContext ctx)
  {
    nodes.put(
        ctx,
        new BinPowExpr(
            ctx.getChild(1).getText(),
            (Expr) nodes.get(ctx.getChild(0)),
            (Expr) nodes.get(ctx.getChild(2))
        )
    );
  }

  @Override
  public void exitFunctionExpr(ExprParser.FunctionExprContext ctx)
  {
    String fnName = ctx.getChild(0).getText();
    final List<Expr> args = ctx.getChildCount() > 3
                            ? (List<Expr>) nodes.get(ctx.getChild(2))
                            : Collections.emptyList();

    Expr expr = macroTable.get(fnName, args);

    if (expr == null) {
      // Built-in functions.
      final Function function = Parser.getFunction(fnName);
      if (function == null) {
        throw new RE("function '%s' is not defined.", fnName);
      }
      expr = new FunctionExpr(function, fnName, args);
    }

    nodes.put(ctx, expr);
  }

  @Override
  public void exitIdentifierExpr(ExprParser.IdentifierExprContext ctx)
  {
    String text = ctx.getText();
    if (text.charAt(0) == '"' && text.charAt(text.length() - 1) == '"') {
      text = StringEscapeUtils.unescapeJava(text.substring(1, text.length() - 1));
    }
    nodes.put(
        ctx,
        new IdentifierExpr(text)
    );
  }

  @Override
  public void exitFunctionArgs(ExprParser.FunctionArgsContext ctx)
  {
    List<Expr> args = new ArrayList<>();
    args.add((Expr) nodes.get(ctx.getChild(0)));

    if (ctx.getChildCount() > 1) {
      for (int i = 1; i <= ctx.getChildCount() / 2; i++) {
        args.add((Expr) nodes.get(ctx.getChild(2 * i)));
      }
    }

    nodes.put(ctx, args);
  }
}
