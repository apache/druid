// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

grammar Expr;

expr : NULL                                                         # null
     | ('-'|'!') expr                                               # unaryOpExpr
     |<assoc=right> expr '^' expr                                   # powOpExpr
     | expr ('*'|'/'|'%') expr                                      # mulDivModuloExpr
     | expr ('+'|'-') expr                                          # addSubExpr
     | expr ('<'|'<='|'>'|'>='|'=='|'!=') expr                      # logicalOpExpr
     | expr ('&&'|'||') expr                                        # logicalAndOrExpr
     | '(' expr ')'                                                 # nestedExpr
     | IDENTIFIER '(' lambda ',' fnArgs ')'                         # applyFunctionExpr
     | IDENTIFIER '(' fnArgs? ')'                                   # functionExpr
     | IDENTIFIER                                                   # identifierExpr
     | DOUBLE                                                       # doubleExpr
     | LONG                                                         # longExpr
     | STRING                                                       # string
     | '[' (stringElement (',' stringElement)*)? ']'                # stringArray
     | '[' longElement (',' longElement)*']'                        # longArray
     | '<LONG>' '[' (numericElement (',' numericElement)*)? ']'     # explicitLongArray
     | '<DOUBLE>'? '[' (numericElement (',' numericElement)*)? ']'  # doubleArray
     | '<STRING>' '[' (literalElement (',' literalElement)*)? ']'   # explicitStringArray
     ;

lambda : (IDENTIFIER | '(' ')' | '(' IDENTIFIER (',' IDENTIFIER)* ')') '->' expr
       ;

fnArgs : expr (',' expr)*                                           # functionArgs
       ;

stringElement : (STRING | NULL);

longElement : (LONG | NULL);

numericElement : (LONG | DOUBLE | NULL);

literalElement : (STRING | LONG | DOUBLE | NULL);

NULL : 'null';
LONG : [0-9]+;
EXP: [eE] [-]? LONG;
// DOUBLE provides partial support for java double format
// see: https://docs.oracle.com/javase/8/docs/api/java/lang/Double.html#valueOf-java.lang.String-
DOUBLE : 'NaN' | 'Infinity' | (LONG '.' LONG?) | (LONG EXP) | (LONG '.' LONG? EXP);
IDENTIFIER : [_$a-zA-Z][_$a-zA-Z0-9]* | '"' (ESC | ~ [\"\\])* '"';
WS : [ \t\r\n]+ -> skip ;

STRING : '\'' (ESC | ~ [\'\\])* '\'';
fragment ESC : '\\' ([\'\"\\/bfnrt] | UNICODE) ;
fragment UNICODE : 'u' HEX HEX HEX HEX ;
fragment HEX : [0-9a-fA-F] ;

MINUS : '-' ;
NOT : '!' ;
POW : '^' ;
MUL : '*' ;
DIV : '/' ;
MODULO : '%' ;
PLUS : '+' ;
LT : '<' ;
LEQ : '<=' ;
GT : '>' ;
GEQ : '>=' ;
EQ : '==' ;
NEQ : '!=' ;
AND : '&&' ;
OR : '||' ;
