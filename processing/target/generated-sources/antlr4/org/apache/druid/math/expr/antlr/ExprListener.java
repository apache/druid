// Generated from org/apache/druid/math/expr/antlr/Expr.g4 by ANTLR 4.9.3
package org.apache.druid.math.expr.antlr;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link ExprParser}.
 */
public interface ExprListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link ExprParser#start}.
	 * @param ctx the parse tree
	 */
	void enterStart(ExprParser.StartContext ctx);
	/**
	 * Exit a parse tree produced by {@link ExprParser#start}.
	 * @param ctx the parse tree
	 */
	void exitStart(ExprParser.StartContext ctx);
	/**
	 * Enter a parse tree produced by the {@code applyFunctionExpr}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterApplyFunctionExpr(ExprParser.ApplyFunctionExprContext ctx);
	/**
	 * Exit a parse tree produced by the {@code applyFunctionExpr}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitApplyFunctionExpr(ExprParser.ApplyFunctionExprContext ctx);
	/**
	 * Enter a parse tree produced by the {@code doubleExpr}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterDoubleExpr(ExprParser.DoubleExprContext ctx);
	/**
	 * Exit a parse tree produced by the {@code doubleExpr}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitDoubleExpr(ExprParser.DoubleExprContext ctx);
	/**
	 * Enter a parse tree produced by the {@code doubleArray}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterDoubleArray(ExprParser.DoubleArrayContext ctx);
	/**
	 * Exit a parse tree produced by the {@code doubleArray}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitDoubleArray(ExprParser.DoubleArrayContext ctx);
	/**
	 * Enter a parse tree produced by the {@code addSubExpr}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterAddSubExpr(ExprParser.AddSubExprContext ctx);
	/**
	 * Exit a parse tree produced by the {@code addSubExpr}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitAddSubExpr(ExprParser.AddSubExprContext ctx);
	/**
	 * Enter a parse tree produced by the {@code string}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterString(ExprParser.StringContext ctx);
	/**
	 * Exit a parse tree produced by the {@code string}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitString(ExprParser.StringContext ctx);
	/**
	 * Enter a parse tree produced by the {@code longExpr}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterLongExpr(ExprParser.LongExprContext ctx);
	/**
	 * Exit a parse tree produced by the {@code longExpr}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitLongExpr(ExprParser.LongExprContext ctx);
	/**
	 * Enter a parse tree produced by the {@code explicitStringArray}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterExplicitStringArray(ExprParser.ExplicitStringArrayContext ctx);
	/**
	 * Exit a parse tree produced by the {@code explicitStringArray}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitExplicitStringArray(ExprParser.ExplicitStringArrayContext ctx);
	/**
	 * Enter a parse tree produced by the {@code logicalAndOrExpr}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterLogicalAndOrExpr(ExprParser.LogicalAndOrExprContext ctx);
	/**
	 * Exit a parse tree produced by the {@code logicalAndOrExpr}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitLogicalAndOrExpr(ExprParser.LogicalAndOrExprContext ctx);
	/**
	 * Enter a parse tree produced by the {@code longArray}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterLongArray(ExprParser.LongArrayContext ctx);
	/**
	 * Exit a parse tree produced by the {@code longArray}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitLongArray(ExprParser.LongArrayContext ctx);
	/**
	 * Enter a parse tree produced by the {@code nestedExpr}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterNestedExpr(ExprParser.NestedExprContext ctx);
	/**
	 * Exit a parse tree produced by the {@code nestedExpr}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitNestedExpr(ExprParser.NestedExprContext ctx);
	/**
	 * Enter a parse tree produced by the {@code logicalOpExpr}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterLogicalOpExpr(ExprParser.LogicalOpExprContext ctx);
	/**
	 * Exit a parse tree produced by the {@code logicalOpExpr}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitLogicalOpExpr(ExprParser.LogicalOpExprContext ctx);
	/**
	 * Enter a parse tree produced by the {@code functionExpr}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterFunctionExpr(ExprParser.FunctionExprContext ctx);
	/**
	 * Exit a parse tree produced by the {@code functionExpr}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitFunctionExpr(ExprParser.FunctionExprContext ctx);
	/**
	 * Enter a parse tree produced by the {@code explicitLongArray}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterExplicitLongArray(ExprParser.ExplicitLongArrayContext ctx);
	/**
	 * Exit a parse tree produced by the {@code explicitLongArray}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitExplicitLongArray(ExprParser.ExplicitLongArrayContext ctx);
	/**
	 * Enter a parse tree produced by the {@code unaryOpExpr}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterUnaryOpExpr(ExprParser.UnaryOpExprContext ctx);
	/**
	 * Exit a parse tree produced by the {@code unaryOpExpr}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitUnaryOpExpr(ExprParser.UnaryOpExprContext ctx);
	/**
	 * Enter a parse tree produced by the {@code null}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterNull(ExprParser.NullContext ctx);
	/**
	 * Exit a parse tree produced by the {@code null}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitNull(ExprParser.NullContext ctx);
	/**
	 * Enter a parse tree produced by the {@code explicitArray}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterExplicitArray(ExprParser.ExplicitArrayContext ctx);
	/**
	 * Exit a parse tree produced by the {@code explicitArray}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitExplicitArray(ExprParser.ExplicitArrayContext ctx);
	/**
	 * Enter a parse tree produced by the {@code stringArray}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterStringArray(ExprParser.StringArrayContext ctx);
	/**
	 * Exit a parse tree produced by the {@code stringArray}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitStringArray(ExprParser.StringArrayContext ctx);
	/**
	 * Enter a parse tree produced by the {@code mulDivModuloExpr}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterMulDivModuloExpr(ExprParser.MulDivModuloExprContext ctx);
	/**
	 * Exit a parse tree produced by the {@code mulDivModuloExpr}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitMulDivModuloExpr(ExprParser.MulDivModuloExprContext ctx);
	/**
	 * Enter a parse tree produced by the {@code powOpExpr}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterPowOpExpr(ExprParser.PowOpExprContext ctx);
	/**
	 * Exit a parse tree produced by the {@code powOpExpr}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitPowOpExpr(ExprParser.PowOpExprContext ctx);
	/**
	 * Enter a parse tree produced by the {@code identifierExpr}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterIdentifierExpr(ExprParser.IdentifierExprContext ctx);
	/**
	 * Exit a parse tree produced by the {@code identifierExpr}
	 * labeled alternative in {@link ExprParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitIdentifierExpr(ExprParser.IdentifierExprContext ctx);
	/**
	 * Enter a parse tree produced by {@link ExprParser#lambda}.
	 * @param ctx the parse tree
	 */
	void enterLambda(ExprParser.LambdaContext ctx);
	/**
	 * Exit a parse tree produced by {@link ExprParser#lambda}.
	 * @param ctx the parse tree
	 */
	void exitLambda(ExprParser.LambdaContext ctx);
	/**
	 * Enter a parse tree produced by the {@code functionArgs}
	 * labeled alternative in {@link ExprParser#fnArgs}.
	 * @param ctx the parse tree
	 */
	void enterFunctionArgs(ExprParser.FunctionArgsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code functionArgs}
	 * labeled alternative in {@link ExprParser#fnArgs}.
	 * @param ctx the parse tree
	 */
	void exitFunctionArgs(ExprParser.FunctionArgsContext ctx);
	/**
	 * Enter a parse tree produced by {@link ExprParser#stringElement}.
	 * @param ctx the parse tree
	 */
	void enterStringElement(ExprParser.StringElementContext ctx);
	/**
	 * Exit a parse tree produced by {@link ExprParser#stringElement}.
	 * @param ctx the parse tree
	 */
	void exitStringElement(ExprParser.StringElementContext ctx);
	/**
	 * Enter a parse tree produced by {@link ExprParser#longElement}.
	 * @param ctx the parse tree
	 */
	void enterLongElement(ExprParser.LongElementContext ctx);
	/**
	 * Exit a parse tree produced by {@link ExprParser#longElement}.
	 * @param ctx the parse tree
	 */
	void exitLongElement(ExprParser.LongElementContext ctx);
	/**
	 * Enter a parse tree produced by {@link ExprParser#numericElement}.
	 * @param ctx the parse tree
	 */
	void enterNumericElement(ExprParser.NumericElementContext ctx);
	/**
	 * Exit a parse tree produced by {@link ExprParser#numericElement}.
	 * @param ctx the parse tree
	 */
	void exitNumericElement(ExprParser.NumericElementContext ctx);
	/**
	 * Enter a parse tree produced by {@link ExprParser#literalElement}.
	 * @param ctx the parse tree
	 */
	void enterLiteralElement(ExprParser.LiteralElementContext ctx);
	/**
	 * Exit a parse tree produced by {@link ExprParser#literalElement}.
	 * @param ctx the parse tree
	 */
	void exitLiteralElement(ExprParser.LiteralElementContext ctx);
}