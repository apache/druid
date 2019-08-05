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

package org.apache.druid.math.expr;

import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.druid.annotations.UsedInGeneratedCode;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.antlr.ExprBaseListener;
import org.apache.druid.math.expr.antlr.ExprParser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of antlr parse tree listener, transforms {@link ParseTree} to {@link Expr}, based on the grammar
 * defined in <a href="../../../../../../src/main/antlr4/org/apache/druid/math/expr/antlr/Expr.g4">Expr.g4</a>. All
 * {@link Expr} are created on 'exit' so that children {@link Expr} are already constructed.
 */
public class ExprListenerImpl extends ExprBaseListener
{
  private final Map<ParseTree, Object> nodes;
  private final ExprMacroTable macroTable;
  private final ParseTree rootNodeKey;

  private final Set<String> lambdaIdentifiers;
  private final Set<String> uniqueIdentifiers;
  private int uniqueCounter = 0;

  ExprListenerImpl(ParseTree rootNodeKey, ExprMacroTable macroTable)
  {
    this.rootNodeKey = rootNodeKey;
    this.macroTable = macroTable;
    this.nodes = new HashMap<>();
    this.lambdaIdentifiers = new HashSet<>();
    this.uniqueIdentifiers = new HashSet<>();
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
  public void exitApplyFunctionExpr(ExprParser.ApplyFunctionExprContext ctx)
  {
    String fnName = ctx.getChild(0).getText();
    // Built-in functions.
    final ApplyFunction function = Parser.getApplyFunction(fnName);
    if (function == null) {
      throw new RE("function '%s' is not defined.", fnName);
    }

    nodes.put(
        ctx,
        new ApplyFunctionExpr(function, fnName, (LambdaExpr) nodes.get(ctx.lambda()), (List<Expr>) nodes.get(ctx.fnArgs()))
    );
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
  public void exitDoubleArray(ExprParser.DoubleArrayContext ctx)
  {
    Double[] values = new Double[ctx.DOUBLE().size()];
    for (int i = 0; i < values.length; i++) {
      values[i] = Double.parseDouble(ctx.DOUBLE(i).getText());
    }
    nodes.put(ctx, new DoubleArrayExpr(values));
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
  public void exitLongArray(ExprParser.LongArrayContext ctx)
  {
    Long[] values = new Long[ctx.LONG().size()];
    for (int i = 0; i < values.length; i++) {
      values[i] = Long.parseLong(ctx.LONG(i).getText());
    }
    nodes.put(ctx, new LongArrayExpr(values));
  }

  @Override
  public void exitNestedExpr(ExprParser.NestedExprContext ctx)
  {
    nodes.put(ctx, nodes.get(ctx.getChild(1)));
  }

  @Override
  public void exitString(ExprParser.StringContext ctx)
  {
    nodes.put(ctx, new StringExpr(escapeStringLiteral(ctx.getText())));
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

  @UsedInGeneratedCode
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
    final String text = sanitizeIdentifierString(ctx.getText());
    nodes.put(ctx, createIdentifierExpr(text));
  }

  @Override
  public void enterLambda(ExprParser.LambdaContext ctx)
  {
    // mark lambda identifiers on enter, for reference later when creating the IdentifierExpr inside of the lambdas
    for (int i = 0; i < ctx.IDENTIFIER().size(); i++) {
      String text = ctx.IDENTIFIER(i).getText();
      text = sanitizeIdentifierString(text);
      this.lambdaIdentifiers.add(text);
    }
  }

  @Override
  public void exitLambda(ExprParser.LambdaContext ctx)
  {
    List<IdentifierExpr> identifiers = new ArrayList<>(ctx.IDENTIFIER().size());
    for (int i = 0; i < ctx.IDENTIFIER().size(); i++) {
      String text = ctx.IDENTIFIER(i).getText();
      text = sanitizeIdentifierString(text);
      identifiers.add(i, createIdentifierExpr(text));
      // clean up lambda identifier references on exit
      lambdaIdentifiers.remove(text);
    }

    nodes.put(ctx, new LambdaExpr(identifiers, (Expr) nodes.get(ctx.expr())));
  }

  @Override
  public void exitFunctionArgs(ExprParser.FunctionArgsContext ctx)
  {
    List<Expr> args = new ArrayList<>();
    for (ParseTree exprCtx : ctx.expr()) {
      args.add((Expr) nodes.get(exprCtx));
    }

    nodes.put(ctx, args);
  }

  @Override
  public void exitNull(ExprParser.NullContext ctx)
  {
    nodes.put(ctx, new StringExpr(null));
  }

  @Override
  public void exitStringArray(ExprParser.StringArrayContext ctx)
  {
    String[] values = new String[ctx.STRING().size()];
    for (int i = 0; i < values.length; i++) {
      values[i] = escapeStringLiteral(ctx.STRING(i).getText());
    }
    nodes.put(ctx, new StringArrayExpr(values));
  }

  @Override
  public void exitEmptyArray(ExprParser.EmptyArrayContext ctx)
  {
    nodes.put(ctx, new StringArrayExpr(new String[0]));
  }

  /**
   * All {@link IdentifierExpr} that are *not* bound to a {@link LambdaExpr} identifier, will recieve a unique
   * {@link IdentifierExpr#identifier} value which may or may not be the same as the
   * {@link IdentifierExpr#binding} value. {@link LambdaExpr} identifiers however, will always have
   * {@link IdentifierExpr#identifier} be the same as {@link IdentifierExpr#binding} because they have
   * synthetic bindings set at evaluation time. This is done to aid in analysis needed for the automatic expression
   * translation which maps scalar expressions to multi-value inputs. See
   * {@link Parser#applyUnappliedBindings(Expr, Expr.BindingDetails, List)}} for additional details.
   */
  private IdentifierExpr createIdentifierExpr(String binding)
  {
    if (!lambdaIdentifiers.contains(binding)) {
      String uniqueIdentifier = binding;
      while (uniqueIdentifiers.contains(uniqueIdentifier)) {
        uniqueIdentifier = StringUtils.format("%s_%s", binding, uniqueCounter++);
      }
      uniqueIdentifiers.add(uniqueIdentifier);
      return new IdentifierExpr(uniqueIdentifier, binding);
    }
    return new IdentifierExpr(binding);
  }

  /**
   * Remove double quotes from an identifier variable string, returning unqouted identifier
   */
  private static String sanitizeIdentifierString(String text)
  {
    if (text.charAt(0) == '"' && text.charAt(text.length() - 1) == '"') {
      text = StringEscapeUtils.unescapeJava(text.substring(1, text.length() - 1));
    }
    return text;
  }

  /**
   * Remove single quote from a string literal, returning unquoted string value
   */
  private static String escapeStringLiteral(String text)
  {
    String unquoted = text.substring(1, text.length() - 1);
    return unquoted.indexOf('\\') >= 0 ? StringEscapeUtils.unescapeJava(unquoted) : unquoted;
  }
}
