// Generated from org/apache/druid/math/expr/antlr/Expr.g4 by ANTLR 4.5.1
package org.apache.druid.math.expr.antlr;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class ExprLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.5.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		ARRAY_TYPE=10, NULL=11, LONG=12, EXP=13, DOUBLE=14, IDENTIFIER=15, WS=16, 
		STRING=17, MINUS=18, NOT=19, POW=20, MUL=21, DIV=22, MODULO=23, PLUS=24, 
		LT=25, LEQ=26, GT=27, GEQ=28, EQ=29, NEQ=30, AND=31, OR=32;
	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"T__0", "T__1", "T__2", "T__3", "T__4", "T__5", "T__6", "T__7", "T__8", 
		"ARRAY_TYPE", "NULL", "LONG", "EXP", "DOUBLE", "IDENTIFIER", "WS", "STRING", 
		"ESC", "UNICODE", "HEX", "MINUS", "NOT", "POW", "MUL", "DIV", "MODULO", 
		"PLUS", "LT", "LEQ", "GT", "GEQ", "EQ", "NEQ", "AND", "OR"
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


	public ExprLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "Expr.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public static final String _serializedATN =
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2\"\u0117\b\1\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\3\2\3\2\3\3\3\3\3\4\3\4\3\5\3\5\3\6\3\6\3\7\3"+
		"\7\3\7\3\7\3\7\3\7\3\7\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\t\3\t\3\t"+
		"\3\t\3\t\3\t\3\t\3\t\3\t\3\n\3\n\3\n\3\13\3\13\3\13\3\13\3\13\3\13\3\13"+
		"\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13"+
		"\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13"+
		"\3\13\5\13\u0094\n\13\3\13\3\13\3\f\3\f\3\f\3\f\3\f\3\r\6\r\u009e\n\r"+
		"\r\r\16\r\u009f\3\16\3\16\5\16\u00a4\n\16\3\16\3\16\3\17\3\17\3\17\3\17"+
		"\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\5\17\u00b6\n\17\3\17"+
		"\3\17\3\17\3\17\3\17\3\17\5\17\u00be\n\17\3\17\3\17\5\17\u00c2\n\17\3"+
		"\20\3\20\7\20\u00c6\n\20\f\20\16\20\u00c9\13\20\3\20\3\20\3\20\7\20\u00ce"+
		"\n\20\f\20\16\20\u00d1\13\20\3\20\5\20\u00d4\n\20\3\21\6\21\u00d7\n\21"+
		"\r\21\16\21\u00d8\3\21\3\21\3\22\3\22\3\22\7\22\u00e0\n\22\f\22\16\22"+
		"\u00e3\13\22\3\22\3\22\3\23\3\23\3\23\5\23\u00ea\n\23\3\24\3\24\3\24\3"+
		"\24\3\24\3\24\3\25\3\25\3\26\3\26\3\27\3\27\3\30\3\30\3\31\3\31\3\32\3"+
		"\32\3\33\3\33\3\34\3\34\3\35\3\35\3\36\3\36\3\36\3\37\3\37\3 \3 \3 \3"+
		"!\3!\3!\3\"\3\"\3\"\3#\3#\3#\3$\3$\3$\2\2%\3\3\5\4\7\5\t\6\13\7\r\b\17"+
		"\t\21\n\23\13\25\f\27\r\31\16\33\17\35\20\37\21!\22#\23%\2\'\2)\2+\24"+
		"-\25/\26\61\27\63\30\65\31\67\329\33;\34=\35?\36A\37C E!G\"\3\2\f\3\2"+
		"\62;\4\2GGgg\3\2//\6\2&&C\\aac|\7\2&&\62;C\\aac|\4\2$$^^\5\2\13\f\17\17"+
		"\"\"\4\2))^^\13\2$$))\61\61^^ddhhppttvv\5\2\62;CHch\u0127\2\3\3\2\2\2"+
		"\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2"+
		"\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2"+
		"\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3\2\2\2\2!\3\2\2\2\2#\3\2\2\2\2+\3\2\2"+
		"\2\2-\3\2\2\2\2/\3\2\2\2\2\61\3\2\2\2\2\63\3\2\2\2\2\65\3\2\2\2\2\67\3"+
		"\2\2\2\29\3\2\2\2\2;\3\2\2\2\2=\3\2\2\2\2?\3\2\2\2\2A\3\2\2\2\2C\3\2\2"+
		"\2\2E\3\2\2\2\2G\3\2\2\2\3I\3\2\2\2\5K\3\2\2\2\7M\3\2\2\2\tO\3\2\2\2\13"+
		"Q\3\2\2\2\rS\3\2\2\2\17Z\3\2\2\2\21c\3\2\2\2\23l\3\2\2\2\25o\3\2\2\2\27"+
		"\u0097\3\2\2\2\31\u009d\3\2\2\2\33\u00a1\3\2\2\2\35\u00c1\3\2\2\2\37\u00d3"+
		"\3\2\2\2!\u00d6\3\2\2\2#\u00dc\3\2\2\2%\u00e6\3\2\2\2\'\u00eb\3\2\2\2"+
		")\u00f1\3\2\2\2+\u00f3\3\2\2\2-\u00f5\3\2\2\2/\u00f7\3\2\2\2\61\u00f9"+
		"\3\2\2\2\63\u00fb\3\2\2\2\65\u00fd\3\2\2\2\67\u00ff\3\2\2\29\u0101\3\2"+
		"\2\2;\u0103\3\2\2\2=\u0106\3\2\2\2?\u0108\3\2\2\2A\u010b\3\2\2\2C\u010e"+
		"\3\2\2\2E\u0111\3\2\2\2G\u0114\3\2\2\2IJ\7*\2\2J\4\3\2\2\2KL\7+\2\2L\6"+
		"\3\2\2\2MN\7.\2\2N\b\3\2\2\2OP\7]\2\2P\n\3\2\2\2QR\7_\2\2R\f\3\2\2\2S"+
		"T\7>\2\2TU\7N\2\2UV\7Q\2\2VW\7P\2\2WX\7I\2\2XY\7@\2\2Y\16\3\2\2\2Z[\7"+
		">\2\2[\\\7F\2\2\\]\7Q\2\2]^\7W\2\2^_\7D\2\2_`\7N\2\2`a\7G\2\2ab\7@\2\2"+
		"b\20\3\2\2\2cd\7>\2\2de\7U\2\2ef\7V\2\2fg\7T\2\2gh\7K\2\2hi\7P\2\2ij\7"+
		"I\2\2jk\7@\2\2k\22\3\2\2\2lm\7/\2\2mn\7@\2\2n\24\3\2\2\2op\7C\2\2pq\7"+
		"T\2\2qr\7T\2\2rs\7C\2\2st\7[\2\2tu\7>\2\2u\u0093\3\2\2\2vw\7N\2\2wx\7"+
		"Q\2\2xy\7P\2\2y\u0094\7I\2\2z{\7F\2\2{|\7Q\2\2|}\7W\2\2}~\7D\2\2~\177"+
		"\7N\2\2\177\u0094\7G\2\2\u0080\u0081\7U\2\2\u0081\u0082\7V\2\2\u0082\u0083"+
		"\7T\2\2\u0083\u0084\7K\2\2\u0084\u0085\7P\2\2\u0085\u0094\7I\2\2\u0086"+
		"\u0087\7E\2\2\u0087\u0088\7Q\2\2\u0088\u0089\7O\2\2\u0089\u008a\7R\2\2"+
		"\u008a\u008b\7N\2\2\u008b\u008c\7G\2\2\u008c\u008d\7Z\2\2\u008d\u008e"+
		"\7>\2\2\u008e\u008f\3\2\2\2\u008f\u0090\5\37\20\2\u0090\u0091\7@\2\2\u0091"+
		"\u0094\3\2\2\2\u0092\u0094\5\25\13\2\u0093v\3\2\2\2\u0093z\3\2\2\2\u0093"+
		"\u0080\3\2\2\2\u0093\u0086\3\2\2\2\u0093\u0092\3\2\2\2\u0094\u0095\3\2"+
		"\2\2\u0095\u0096\7@\2\2\u0096\26\3\2\2\2\u0097\u0098\7p\2\2\u0098\u0099"+
		"\7w\2\2\u0099\u009a\7n\2\2\u009a\u009b\7n\2\2\u009b\30\3\2\2\2\u009c\u009e"+
		"\t\2\2\2\u009d\u009c\3\2\2\2\u009e\u009f\3\2\2\2\u009f\u009d\3\2\2\2\u009f"+
		"\u00a0\3\2\2\2\u00a0\32\3\2\2\2\u00a1\u00a3\t\3\2\2\u00a2\u00a4\t\4\2"+
		"\2\u00a3\u00a2\3\2\2\2\u00a3\u00a4\3\2\2\2\u00a4\u00a5\3\2\2\2\u00a5\u00a6"+
		"\5\31\r\2\u00a6\34\3\2\2\2\u00a7\u00a8\7P\2\2\u00a8\u00a9\7c\2\2\u00a9"+
		"\u00c2\7P\2\2\u00aa\u00ab\7K\2\2\u00ab\u00ac\7p\2\2\u00ac\u00ad\7h\2\2"+
		"\u00ad\u00ae\7k\2\2\u00ae\u00af\7p\2\2\u00af\u00b0\7k\2\2\u00b0\u00b1"+
		"\7v\2\2\u00b1\u00c2\7{\2\2\u00b2\u00b3\5\31\r\2\u00b3\u00b5\7\60\2\2\u00b4"+
		"\u00b6\5\31\r\2\u00b5\u00b4\3\2\2\2\u00b5\u00b6\3\2\2\2\u00b6\u00c2\3"+
		"\2\2\2\u00b7\u00b8\5\31\r\2\u00b8\u00b9\5\33\16\2\u00b9\u00c2\3\2\2\2"+
		"\u00ba\u00bb\5\31\r\2\u00bb\u00bd\7\60\2\2\u00bc\u00be\5\31\r\2\u00bd"+
		"\u00bc\3\2\2\2\u00bd\u00be\3\2\2\2\u00be\u00bf\3\2\2\2\u00bf\u00c0\5\33"+
		"\16\2\u00c0\u00c2\3\2\2\2\u00c1\u00a7\3\2\2\2\u00c1\u00aa\3\2\2\2\u00c1"+
		"\u00b2\3\2\2\2\u00c1\u00b7\3\2\2\2\u00c1\u00ba\3\2\2\2\u00c2\36\3\2\2"+
		"\2\u00c3\u00c7\t\5\2\2\u00c4\u00c6\t\6\2\2\u00c5\u00c4\3\2\2\2\u00c6\u00c9"+
		"\3\2\2\2\u00c7\u00c5\3\2\2\2\u00c7\u00c8\3\2\2\2\u00c8\u00d4\3\2\2\2\u00c9"+
		"\u00c7\3\2\2\2\u00ca\u00cf\7$\2\2\u00cb\u00ce\5%\23\2\u00cc\u00ce\n\7"+
		"\2\2\u00cd\u00cb\3\2\2\2\u00cd\u00cc\3\2\2\2\u00ce\u00d1\3\2\2\2\u00cf"+
		"\u00cd\3\2\2\2\u00cf\u00d0\3\2\2\2\u00d0\u00d2\3\2\2\2\u00d1\u00cf\3\2"+
		"\2\2\u00d2\u00d4\7$\2\2\u00d3\u00c3\3\2\2\2\u00d3\u00ca\3\2\2\2\u00d4"+
		" \3\2\2\2\u00d5\u00d7\t\b\2\2\u00d6\u00d5\3\2\2\2\u00d7\u00d8\3\2\2\2"+
		"\u00d8\u00d6\3\2\2\2\u00d8\u00d9\3\2\2\2\u00d9\u00da\3\2\2\2\u00da\u00db"+
		"\b\21\2\2\u00db\"\3\2\2\2\u00dc\u00e1\7)\2\2\u00dd\u00e0\5%\23\2\u00de"+
		"\u00e0\n\t\2\2\u00df\u00dd\3\2\2\2\u00df\u00de\3\2\2\2\u00e0\u00e3\3\2"+
		"\2\2\u00e1\u00df\3\2\2\2\u00e1\u00e2\3\2\2\2\u00e2\u00e4\3\2\2\2\u00e3"+
		"\u00e1\3\2\2\2\u00e4\u00e5\7)\2\2\u00e5$\3\2\2\2\u00e6\u00e9\7^\2\2\u00e7"+
		"\u00ea\t\n\2\2\u00e8\u00ea\5\'\24\2\u00e9\u00e7\3\2\2\2\u00e9\u00e8\3"+
		"\2\2\2\u00ea&\3\2\2\2\u00eb\u00ec\7w\2\2\u00ec\u00ed\5)\25\2\u00ed\u00ee"+
		"\5)\25\2\u00ee\u00ef\5)\25\2\u00ef\u00f0\5)\25\2\u00f0(\3\2\2\2\u00f1"+
		"\u00f2\t\13\2\2\u00f2*\3\2\2\2\u00f3\u00f4\7/\2\2\u00f4,\3\2\2\2\u00f5"+
		"\u00f6\7#\2\2\u00f6.\3\2\2\2\u00f7\u00f8\7`\2\2\u00f8\60\3\2\2\2\u00f9"+
		"\u00fa\7,\2\2\u00fa\62\3\2\2\2\u00fb\u00fc\7\61\2\2\u00fc\64\3\2\2\2\u00fd"+
		"\u00fe\7\'\2\2\u00fe\66\3\2\2\2\u00ff\u0100\7-\2\2\u01008\3\2\2\2\u0101"+
		"\u0102\7>\2\2\u0102:\3\2\2\2\u0103\u0104\7>\2\2\u0104\u0105\7?\2\2\u0105"+
		"<\3\2\2\2\u0106\u0107\7@\2\2\u0107>\3\2\2\2\u0108\u0109\7@\2\2\u0109\u010a"+
		"\7?\2\2\u010a@\3\2\2\2\u010b\u010c\7?\2\2\u010c\u010d\7?\2\2\u010dB\3"+
		"\2\2\2\u010e\u010f\7#\2\2\u010f\u0110\7?\2\2\u0110D\3\2\2\2\u0111\u0112"+
		"\7(\2\2\u0112\u0113\7(\2\2\u0113F\3\2\2\2\u0114\u0115\7~\2\2\u0115\u0116"+
		"\7~\2\2\u0116H\3\2\2\2\21\2\u0093\u009f\u00a3\u00b5\u00bd\u00c1\u00c7"+
		"\u00cd\u00cf\u00d3\u00d8\u00df\u00e1\u00e9\3\b\2\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}