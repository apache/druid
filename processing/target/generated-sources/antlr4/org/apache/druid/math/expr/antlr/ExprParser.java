// Generated from org/apache/druid/math/expr/antlr/Expr.g4 by ANTLR 4.5.1
package org.apache.druid.math.expr.antlr;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class ExprParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.5.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		ARRAY_TYPE=10, NULL=11, LONG=12, EXP=13, DOUBLE=14, IDENTIFIER=15, WS=16, 
		STRING=17, MINUS=18, NOT=19, POW=20, MUL=21, DIV=22, MODULO=23, PLUS=24, 
		LT=25, LEQ=26, GT=27, GEQ=28, EQ=29, NEQ=30, AND=31, OR=32;
	public static final int
		RULE_start = 0, RULE_expr = 1, RULE_lambda = 2, RULE_fnArgs = 3, RULE_stringElement = 4, 
		RULE_longElement = 5, RULE_numericElement = 6, RULE_literalElement = 7;
	public static final String[] ruleNames = {
		"start", "expr", "lambda", "fnArgs", "stringElement", "longElement", "numericElement", 
		"literalElement"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'('", "')'", "','", "'['", "']'", "'<LONG>'", "'<DOUBLE>'", "'<STRING>'", 
		"'->'", null, "'null'", null, null, null, null, null, null, "'-'", "'!'", 
		"'^'", "'*'", "'/'", "'%'", "'+'", "'<'", "'<='", "'>'", "'>='", "'=='", 
		"'!='", "'&&'", "'||'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, null, null, null, null, "ARRAY_TYPE", 
		"NULL", "LONG", "EXP", "DOUBLE", "IDENTIFIER", "WS", "STRING", "MINUS", 
		"NOT", "POW", "MUL", "DIV", "MODULO", "PLUS", "LT", "LEQ", "GT", "GEQ", 
		"EQ", "NEQ", "AND", "OR"
	};
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "Expr.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public ExprParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}
	public static class StartContext extends ParserRuleContext {
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode EOF() { return getToken(ExprParser.EOF, 0); }
		public StartContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_start; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).enterStart(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).exitStart(this);
		}
	}

	public final StartContext start() throws RecognitionException {
		StartContext _localctx = new StartContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_start);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(16);
			expr(0);
			setState(17);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExprContext extends ParserRuleContext {
		public ExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expr; }
	 
		public ExprContext() { }
		public void copyFrom(ExprContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class ApplyFunctionExprContext extends ExprContext {
		public TerminalNode IDENTIFIER() { return getToken(ExprParser.IDENTIFIER, 0); }
		public LambdaContext lambda() {
			return getRuleContext(LambdaContext.class,0);
		}
		public FnArgsContext fnArgs() {
			return getRuleContext(FnArgsContext.class,0);
		}
		public ApplyFunctionExprContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).enterApplyFunctionExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).exitApplyFunctionExpr(this);
		}
	}
	public static class DoubleExprContext extends ExprContext {
		public TerminalNode DOUBLE() { return getToken(ExprParser.DOUBLE, 0); }
		public DoubleExprContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).enterDoubleExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).exitDoubleExpr(this);
		}
	}
	public static class DoubleArrayContext extends ExprContext {
		public List<NumericElementContext> numericElement() {
			return getRuleContexts(NumericElementContext.class);
		}
		public NumericElementContext numericElement(int i) {
			return getRuleContext(NumericElementContext.class,i);
		}
		public DoubleArrayContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).enterDoubleArray(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).exitDoubleArray(this);
		}
	}
	public static class AddSubExprContext extends ExprContext {
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public AddSubExprContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).enterAddSubExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).exitAddSubExpr(this);
		}
	}
	public static class StringContext extends ExprContext {
		public TerminalNode STRING() { return getToken(ExprParser.STRING, 0); }
		public StringContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).enterString(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).exitString(this);
		}
	}
	public static class LongExprContext extends ExprContext {
		public TerminalNode LONG() { return getToken(ExprParser.LONG, 0); }
		public LongExprContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).enterLongExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).exitLongExpr(this);
		}
	}
	public static class ExplicitStringArrayContext extends ExprContext {
		public List<LiteralElementContext> literalElement() {
			return getRuleContexts(LiteralElementContext.class);
		}
		public LiteralElementContext literalElement(int i) {
			return getRuleContext(LiteralElementContext.class,i);
		}
		public ExplicitStringArrayContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).enterExplicitStringArray(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).exitExplicitStringArray(this);
		}
	}
	public static class LogicalAndOrExprContext extends ExprContext {
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public LogicalAndOrExprContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).enterLogicalAndOrExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).exitLogicalAndOrExpr(this);
		}
	}
	public static class LongArrayContext extends ExprContext {
		public List<LongElementContext> longElement() {
			return getRuleContexts(LongElementContext.class);
		}
		public LongElementContext longElement(int i) {
			return getRuleContext(LongElementContext.class,i);
		}
		public LongArrayContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).enterLongArray(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).exitLongArray(this);
		}
	}
	public static class NestedExprContext extends ExprContext {
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public NestedExprContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).enterNestedExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).exitNestedExpr(this);
		}
	}
	public static class LogicalOpExprContext extends ExprContext {
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public LogicalOpExprContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).enterLogicalOpExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).exitLogicalOpExpr(this);
		}
	}
	public static class FunctionExprContext extends ExprContext {
		public TerminalNode IDENTIFIER() { return getToken(ExprParser.IDENTIFIER, 0); }
		public FnArgsContext fnArgs() {
			return getRuleContext(FnArgsContext.class,0);
		}
		public FunctionExprContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).enterFunctionExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).exitFunctionExpr(this);
		}
	}
	public static class ExplicitLongArrayContext extends ExprContext {
		public List<NumericElementContext> numericElement() {
			return getRuleContexts(NumericElementContext.class);
		}
		public NumericElementContext numericElement(int i) {
			return getRuleContext(NumericElementContext.class,i);
		}
		public ExplicitLongArrayContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).enterExplicitLongArray(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).exitExplicitLongArray(this);
		}
	}
	public static class UnaryOpExprContext extends ExprContext {
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public UnaryOpExprContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).enterUnaryOpExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).exitUnaryOpExpr(this);
		}
	}
	public static class NullContext extends ExprContext {
		public TerminalNode NULL() { return getToken(ExprParser.NULL, 0); }
		public NullContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).enterNull(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).exitNull(this);
		}
	}
	public static class ExplicitArrayContext extends ExprContext {
		public TerminalNode ARRAY_TYPE() { return getToken(ExprParser.ARRAY_TYPE, 0); }
		public List<LiteralElementContext> literalElement() {
			return getRuleContexts(LiteralElementContext.class);
		}
		public LiteralElementContext literalElement(int i) {
			return getRuleContext(LiteralElementContext.class,i);
		}
		public ExplicitArrayContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).enterExplicitArray(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).exitExplicitArray(this);
		}
	}
	public static class StringArrayContext extends ExprContext {
		public List<StringElementContext> stringElement() {
			return getRuleContexts(StringElementContext.class);
		}
		public StringElementContext stringElement(int i) {
			return getRuleContext(StringElementContext.class,i);
		}
		public StringArrayContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).enterStringArray(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).exitStringArray(this);
		}
	}
	public static class MulDivModuloExprContext extends ExprContext {
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public MulDivModuloExprContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).enterMulDivModuloExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).exitMulDivModuloExpr(this);
		}
	}
	public static class PowOpExprContext extends ExprContext {
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public PowOpExprContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).enterPowOpExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).exitPowOpExpr(this);
		}
	}
	public static class IdentifierExprContext extends ExprContext {
		public TerminalNode IDENTIFIER() { return getToken(ExprParser.IDENTIFIER, 0); }
		public IdentifierExprContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).enterIdentifierExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).exitIdentifierExpr(this);
		}
	}

	public final ExprContext expr() throws RecognitionException {
		return expr(0);
	}

	private ExprContext expr(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ExprContext _localctx = new ExprContext(_ctx, _parentState);
		ExprContext _prevctx = _localctx;
		int _startState = 2;
		enterRecursionRule(_localctx, 2, RULE_expr, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(121);
			switch ( getInterpreter().adaptivePredict(_input,13,_ctx) ) {
			case 1:
				{
				_localctx = new UnaryOpExprContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(20);
				_la = _input.LA(1);
				if ( !(_la==MINUS || _la==NOT) ) {
				_errHandler.recoverInline(this);
				} else {
					consume();
				}
				setState(21);
				expr(19);
				}
				break;
			case 2:
				{
				_localctx = new NullContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(22);
				match(NULL);
				}
				break;
			case 3:
				{
				_localctx = new NestedExprContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(23);
				match(T__0);
				setState(24);
				expr(0);
				setState(25);
				match(T__1);
				}
				break;
			case 4:
				{
				_localctx = new ApplyFunctionExprContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(27);
				match(IDENTIFIER);
				setState(28);
				match(T__0);
				setState(29);
				lambda();
				setState(30);
				match(T__2);
				setState(31);
				fnArgs();
				setState(32);
				match(T__1);
				}
				break;
			case 5:
				{
				_localctx = new FunctionExprContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(34);
				match(IDENTIFIER);
				setState(35);
				match(T__0);
				setState(37);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << T__3) | (1L << T__5) | (1L << T__6) | (1L << T__7) | (1L << ARRAY_TYPE) | (1L << NULL) | (1L << LONG) | (1L << DOUBLE) | (1L << IDENTIFIER) | (1L << STRING) | (1L << MINUS) | (1L << NOT))) != 0)) {
					{
					setState(36);
					fnArgs();
					}
				}

				setState(39);
				match(T__1);
				}
				break;
			case 6:
				{
				_localctx = new IdentifierExprContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(40);
				match(IDENTIFIER);
				}
				break;
			case 7:
				{
				_localctx = new DoubleExprContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(41);
				match(DOUBLE);
				}
				break;
			case 8:
				{
				_localctx = new LongExprContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(42);
				match(LONG);
				}
				break;
			case 9:
				{
				_localctx = new StringContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(43);
				match(STRING);
				}
				break;
			case 10:
				{
				_localctx = new StringArrayContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(44);
				match(T__3);
				setState(53);
				_la = _input.LA(1);
				if (_la==NULL || _la==STRING) {
					{
					setState(45);
					stringElement();
					setState(50);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(46);
						match(T__2);
						setState(47);
						stringElement();
						}
						}
						setState(52);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(55);
				match(T__4);
				}
				break;
			case 11:
				{
				_localctx = new LongArrayContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(56);
				match(T__3);
				setState(57);
				longElement();
				setState(62);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(58);
					match(T__2);
					setState(59);
					longElement();
					}
					}
					setState(64);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(65);
				match(T__4);
				}
				break;
			case 12:
				{
				_localctx = new ExplicitLongArrayContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(67);
				match(T__5);
				setState(68);
				match(T__3);
				setState(77);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << NULL) | (1L << LONG) | (1L << DOUBLE))) != 0)) {
					{
					setState(69);
					numericElement();
					setState(74);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(70);
						match(T__2);
						setState(71);
						numericElement();
						}
						}
						setState(76);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(79);
				match(T__4);
				}
				break;
			case 13:
				{
				_localctx = new DoubleArrayContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(81);
				_la = _input.LA(1);
				if (_la==T__6) {
					{
					setState(80);
					match(T__6);
					}
				}

				setState(83);
				match(T__3);
				setState(92);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << NULL) | (1L << LONG) | (1L << DOUBLE))) != 0)) {
					{
					setState(84);
					numericElement();
					setState(89);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(85);
						match(T__2);
						setState(86);
						numericElement();
						}
						}
						setState(91);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(94);
				match(T__4);
				}
				break;
			case 14:
				{
				_localctx = new ExplicitStringArrayContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(95);
				match(T__7);
				setState(96);
				match(T__3);
				setState(105);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << NULL) | (1L << LONG) | (1L << DOUBLE) | (1L << STRING))) != 0)) {
					{
					setState(97);
					literalElement();
					setState(102);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(98);
						match(T__2);
						setState(99);
						literalElement();
						}
						}
						setState(104);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(107);
				match(T__4);
				}
				break;
			case 15:
				{
				_localctx = new ExplicitArrayContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(108);
				match(ARRAY_TYPE);
				setState(109);
				match(T__3);
				setState(118);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << NULL) | (1L << LONG) | (1L << DOUBLE) | (1L << STRING))) != 0)) {
					{
					setState(110);
					literalElement();
					setState(115);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(111);
						match(T__2);
						setState(112);
						literalElement();
						}
						}
						setState(117);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(120);
				match(T__4);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(140);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,15,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(138);
					switch ( getInterpreter().adaptivePredict(_input,14,_ctx) ) {
					case 1:
						{
						_localctx = new PowOpExprContext(new ExprContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(123);
						if (!(precpred(_ctx, 18))) throw new FailedPredicateException(this, "precpred(_ctx, 18)");
						setState(124);
						match(POW);
						setState(125);
						expr(18);
						}
						break;
					case 2:
						{
						_localctx = new MulDivModuloExprContext(new ExprContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(126);
						if (!(precpred(_ctx, 17))) throw new FailedPredicateException(this, "precpred(_ctx, 17)");
						setState(127);
						_la = _input.LA(1);
						if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << MUL) | (1L << DIV) | (1L << MODULO))) != 0)) ) {
						_errHandler.recoverInline(this);
						} else {
							consume();
						}
						setState(128);
						expr(18);
						}
						break;
					case 3:
						{
						_localctx = new AddSubExprContext(new ExprContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(129);
						if (!(precpred(_ctx, 16))) throw new FailedPredicateException(this, "precpred(_ctx, 16)");
						setState(130);
						_la = _input.LA(1);
						if ( !(_la==MINUS || _la==PLUS) ) {
						_errHandler.recoverInline(this);
						} else {
							consume();
						}
						setState(131);
						expr(17);
						}
						break;
					case 4:
						{
						_localctx = new LogicalOpExprContext(new ExprContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(132);
						if (!(precpred(_ctx, 15))) throw new FailedPredicateException(this, "precpred(_ctx, 15)");
						setState(133);
						_la = _input.LA(1);
						if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LT) | (1L << LEQ) | (1L << GT) | (1L << GEQ) | (1L << EQ) | (1L << NEQ))) != 0)) ) {
						_errHandler.recoverInline(this);
						} else {
							consume();
						}
						setState(134);
						expr(16);
						}
						break;
					case 5:
						{
						_localctx = new LogicalAndOrExprContext(new ExprContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(135);
						if (!(precpred(_ctx, 14))) throw new FailedPredicateException(this, "precpred(_ctx, 14)");
						setState(136);
						_la = _input.LA(1);
						if ( !(_la==AND || _la==OR) ) {
						_errHandler.recoverInline(this);
						} else {
							consume();
						}
						setState(137);
						expr(15);
						}
						break;
					}
					} 
				}
				setState(142);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,15,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class LambdaContext extends ParserRuleContext {
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public List<TerminalNode> IDENTIFIER() { return getTokens(ExprParser.IDENTIFIER); }
		public TerminalNode IDENTIFIER(int i) {
			return getToken(ExprParser.IDENTIFIER, i);
		}
		public LambdaContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_lambda; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).enterLambda(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).exitLambda(this);
		}
	}

	public final LambdaContext lambda() throws RecognitionException {
		LambdaContext _localctx = new LambdaContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_lambda);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(156);
			switch ( getInterpreter().adaptivePredict(_input,17,_ctx) ) {
			case 1:
				{
				setState(143);
				match(IDENTIFIER);
				}
				break;
			case 2:
				{
				setState(144);
				match(T__0);
				setState(145);
				match(T__1);
				}
				break;
			case 3:
				{
				setState(146);
				match(T__0);
				setState(147);
				match(IDENTIFIER);
				setState(152);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(148);
					match(T__2);
					setState(149);
					match(IDENTIFIER);
					}
					}
					setState(154);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(155);
				match(T__1);
				}
				break;
			}
			setState(158);
			match(T__8);
			setState(159);
			expr(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FnArgsContext extends ParserRuleContext {
		public FnArgsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fnArgs; }
	 
		public FnArgsContext() { }
		public void copyFrom(FnArgsContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class FunctionArgsContext extends FnArgsContext {
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public FunctionArgsContext(FnArgsContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).enterFunctionArgs(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).exitFunctionArgs(this);
		}
	}

	public final FnArgsContext fnArgs() throws RecognitionException {
		FnArgsContext _localctx = new FnArgsContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_fnArgs);
		int _la;
		try {
			_localctx = new FunctionArgsContext(_localctx);
			enterOuterAlt(_localctx, 1);
			{
			setState(161);
			expr(0);
			setState(166);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(162);
				match(T__2);
				setState(163);
				expr(0);
				}
				}
				setState(168);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StringElementContext extends ParserRuleContext {
		public TerminalNode STRING() { return getToken(ExprParser.STRING, 0); }
		public TerminalNode NULL() { return getToken(ExprParser.NULL, 0); }
		public StringElementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_stringElement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).enterStringElement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).exitStringElement(this);
		}
	}

	public final StringElementContext stringElement() throws RecognitionException {
		StringElementContext _localctx = new StringElementContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_stringElement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(169);
			_la = _input.LA(1);
			if ( !(_la==NULL || _la==STRING) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LongElementContext extends ParserRuleContext {
		public TerminalNode LONG() { return getToken(ExprParser.LONG, 0); }
		public TerminalNode NULL() { return getToken(ExprParser.NULL, 0); }
		public LongElementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_longElement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).enterLongElement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).exitLongElement(this);
		}
	}

	public final LongElementContext longElement() throws RecognitionException {
		LongElementContext _localctx = new LongElementContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_longElement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(171);
			_la = _input.LA(1);
			if ( !(_la==NULL || _la==LONG) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NumericElementContext extends ParserRuleContext {
		public TerminalNode LONG() { return getToken(ExprParser.LONG, 0); }
		public TerminalNode DOUBLE() { return getToken(ExprParser.DOUBLE, 0); }
		public TerminalNode NULL() { return getToken(ExprParser.NULL, 0); }
		public NumericElementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_numericElement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).enterNumericElement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).exitNumericElement(this);
		}
	}

	public final NumericElementContext numericElement() throws RecognitionException {
		NumericElementContext _localctx = new NumericElementContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_numericElement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(173);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << NULL) | (1L << LONG) | (1L << DOUBLE))) != 0)) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LiteralElementContext extends ParserRuleContext {
		public TerminalNode STRING() { return getToken(ExprParser.STRING, 0); }
		public TerminalNode LONG() { return getToken(ExprParser.LONG, 0); }
		public TerminalNode DOUBLE() { return getToken(ExprParser.DOUBLE, 0); }
		public TerminalNode NULL() { return getToken(ExprParser.NULL, 0); }
		public LiteralElementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_literalElement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).enterLiteralElement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ExprListener ) ((ExprListener)listener).exitLiteralElement(this);
		}
	}

	public final LiteralElementContext literalElement() throws RecognitionException {
		LiteralElementContext _localctx = new LiteralElementContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_literalElement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(175);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << NULL) | (1L << LONG) | (1L << DOUBLE) | (1L << STRING))) != 0)) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 1:
			return expr_sempred((ExprContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean expr_sempred(ExprContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 18);
		case 1:
			return precpred(_ctx, 17);
		case 2:
			return precpred(_ctx, 16);
		case 3:
			return precpred(_ctx, 15);
		case 4:
			return precpred(_ctx, 14);
		}
		return true;
	}

	public static final String _serializedATN =
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3\"\u00b4\4\2\t\2\4"+
		"\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\3\2\3\2\3\2\3\3"+
		"\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\5"+
		"\3(\n\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\7\3\63\n\3\f\3\16\3\66\13"+
		"\3\5\38\n\3\3\3\3\3\3\3\3\3\3\3\7\3?\n\3\f\3\16\3B\13\3\3\3\3\3\3\3\3"+
		"\3\3\3\3\3\3\3\7\3K\n\3\f\3\16\3N\13\3\5\3P\n\3\3\3\3\3\5\3T\n\3\3\3\3"+
		"\3\3\3\3\3\7\3Z\n\3\f\3\16\3]\13\3\5\3_\n\3\3\3\3\3\3\3\3\3\3\3\3\3\7"+
		"\3g\n\3\f\3\16\3j\13\3\5\3l\n\3\3\3\3\3\3\3\3\3\3\3\3\3\7\3t\n\3\f\3\16"+
		"\3w\13\3\5\3y\n\3\3\3\5\3|\n\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3"+
		"\3\3\3\3\3\3\3\3\3\3\7\3\u008d\n\3\f\3\16\3\u0090\13\3\3\4\3\4\3\4\3\4"+
		"\3\4\3\4\3\4\7\4\u0099\n\4\f\4\16\4\u009c\13\4\3\4\5\4\u009f\n\4\3\4\3"+
		"\4\3\4\3\5\3\5\3\5\7\5\u00a7\n\5\f\5\16\5\u00aa\13\5\3\6\3\6\3\7\3\7\3"+
		"\b\3\b\3\t\3\t\3\t\2\3\4\n\2\4\6\b\n\f\16\20\2\13\3\2\24\25\3\2\27\31"+
		"\4\2\24\24\32\32\3\2\33 \3\2!\"\4\2\r\r\23\23\3\2\r\16\4\2\r\16\20\20"+
		"\5\2\r\16\20\20\23\23\u00cf\2\22\3\2\2\2\4{\3\2\2\2\6\u009e\3\2\2\2\b"+
		"\u00a3\3\2\2\2\n\u00ab\3\2\2\2\f\u00ad\3\2\2\2\16\u00af\3\2\2\2\20\u00b1"+
		"\3\2\2\2\22\23\5\4\3\2\23\24\7\2\2\3\24\3\3\2\2\2\25\26\b\3\1\2\26\27"+
		"\t\2\2\2\27|\5\4\3\25\30|\7\r\2\2\31\32\7\3\2\2\32\33\5\4\3\2\33\34\7"+
		"\4\2\2\34|\3\2\2\2\35\36\7\21\2\2\36\37\7\3\2\2\37 \5\6\4\2 !\7\5\2\2"+
		"!\"\5\b\5\2\"#\7\4\2\2#|\3\2\2\2$%\7\21\2\2%\'\7\3\2\2&(\5\b\5\2\'&\3"+
		"\2\2\2\'(\3\2\2\2()\3\2\2\2)|\7\4\2\2*|\7\21\2\2+|\7\20\2\2,|\7\16\2\2"+
		"-|\7\23\2\2.\67\7\6\2\2/\64\5\n\6\2\60\61\7\5\2\2\61\63\5\n\6\2\62\60"+
		"\3\2\2\2\63\66\3\2\2\2\64\62\3\2\2\2\64\65\3\2\2\2\658\3\2\2\2\66\64\3"+
		"\2\2\2\67/\3\2\2\2\678\3\2\2\289\3\2\2\29|\7\7\2\2:;\7\6\2\2;@\5\f\7\2"+
		"<=\7\5\2\2=?\5\f\7\2><\3\2\2\2?B\3\2\2\2@>\3\2\2\2@A\3\2\2\2AC\3\2\2\2"+
		"B@\3\2\2\2CD\7\7\2\2D|\3\2\2\2EF\7\b\2\2FO\7\6\2\2GL\5\16\b\2HI\7\5\2"+
		"\2IK\5\16\b\2JH\3\2\2\2KN\3\2\2\2LJ\3\2\2\2LM\3\2\2\2MP\3\2\2\2NL\3\2"+
		"\2\2OG\3\2\2\2OP\3\2\2\2PQ\3\2\2\2Q|\7\7\2\2RT\7\t\2\2SR\3\2\2\2ST\3\2"+
		"\2\2TU\3\2\2\2U^\7\6\2\2V[\5\16\b\2WX\7\5\2\2XZ\5\16\b\2YW\3\2\2\2Z]\3"+
		"\2\2\2[Y\3\2\2\2[\\\3\2\2\2\\_\3\2\2\2][\3\2\2\2^V\3\2\2\2^_\3\2\2\2_"+
		"`\3\2\2\2`|\7\7\2\2ab\7\n\2\2bk\7\6\2\2ch\5\20\t\2de\7\5\2\2eg\5\20\t"+
		"\2fd\3\2\2\2gj\3\2\2\2hf\3\2\2\2hi\3\2\2\2il\3\2\2\2jh\3\2\2\2kc\3\2\2"+
		"\2kl\3\2\2\2lm\3\2\2\2m|\7\7\2\2no\7\f\2\2ox\7\6\2\2pu\5\20\t\2qr\7\5"+
		"\2\2rt\5\20\t\2sq\3\2\2\2tw\3\2\2\2us\3\2\2\2uv\3\2\2\2vy\3\2\2\2wu\3"+
		"\2\2\2xp\3\2\2\2xy\3\2\2\2yz\3\2\2\2z|\7\7\2\2{\25\3\2\2\2{\30\3\2\2\2"+
		"{\31\3\2\2\2{\35\3\2\2\2{$\3\2\2\2{*\3\2\2\2{+\3\2\2\2{,\3\2\2\2{-\3\2"+
		"\2\2{.\3\2\2\2{:\3\2\2\2{E\3\2\2\2{S\3\2\2\2{a\3\2\2\2{n\3\2\2\2|\u008e"+
		"\3\2\2\2}~\f\24\2\2~\177\7\26\2\2\177\u008d\5\4\3\24\u0080\u0081\f\23"+
		"\2\2\u0081\u0082\t\3\2\2\u0082\u008d\5\4\3\24\u0083\u0084\f\22\2\2\u0084"+
		"\u0085\t\4\2\2\u0085\u008d\5\4\3\23\u0086\u0087\f\21\2\2\u0087\u0088\t"+
		"\5\2\2\u0088\u008d\5\4\3\22\u0089\u008a\f\20\2\2\u008a\u008b\t\6\2\2\u008b"+
		"\u008d\5\4\3\21\u008c}\3\2\2\2\u008c\u0080\3\2\2\2\u008c\u0083\3\2\2\2"+
		"\u008c\u0086\3\2\2\2\u008c\u0089\3\2\2\2\u008d\u0090\3\2\2\2\u008e\u008c"+
		"\3\2\2\2\u008e\u008f\3\2\2\2\u008f\5\3\2\2\2\u0090\u008e\3\2\2\2\u0091"+
		"\u009f\7\21\2\2\u0092\u0093\7\3\2\2\u0093\u009f\7\4\2\2\u0094\u0095\7"+
		"\3\2\2\u0095\u009a\7\21\2\2\u0096\u0097\7\5\2\2\u0097\u0099\7\21\2\2\u0098"+
		"\u0096\3\2\2\2\u0099\u009c\3\2\2\2\u009a\u0098\3\2\2\2\u009a\u009b\3\2"+
		"\2\2\u009b\u009d\3\2\2\2\u009c\u009a\3\2\2\2\u009d\u009f\7\4\2\2\u009e"+
		"\u0091\3\2\2\2\u009e\u0092\3\2\2\2\u009e\u0094\3\2\2\2\u009f\u00a0\3\2"+
		"\2\2\u00a0\u00a1\7\13\2\2\u00a1\u00a2\5\4\3\2\u00a2\7\3\2\2\2\u00a3\u00a8"+
		"\5\4\3\2\u00a4\u00a5\7\5\2\2\u00a5\u00a7\5\4\3\2\u00a6\u00a4\3\2\2\2\u00a7"+
		"\u00aa\3\2\2\2\u00a8\u00a6\3\2\2\2\u00a8\u00a9\3\2\2\2\u00a9\t\3\2\2\2"+
		"\u00aa\u00a8\3\2\2\2\u00ab\u00ac\t\7\2\2\u00ac\13\3\2\2\2\u00ad\u00ae"+
		"\t\b\2\2\u00ae\r\3\2\2\2\u00af\u00b0\t\t\2\2\u00b0\17\3\2\2\2\u00b1\u00b2"+
		"\t\n\2\2\u00b2\21\3\2\2\2\25\'\64\67@LOS[^hkux{\u008c\u008e\u009a\u009e"+
		"\u00a8";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}