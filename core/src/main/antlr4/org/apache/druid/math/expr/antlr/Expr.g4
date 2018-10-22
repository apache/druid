grammar Expr;

expr : 'null'                                         # null
     | ('-'|'!') expr                                 # unaryOpExpr
     |<assoc=right> expr '^' expr                     # powOpExpr
     | expr ('*'|'/'|'%') expr                        # mulDivModuloExpr
     | expr ('+'|'-') expr                            # addSubExpr
     | expr ('<'|'<='|'>'|'>='|'=='|'!=') expr        # logicalOpExpr
     | expr ('&&'|'||') expr                          # logicalAndOrExpr
     | '(' expr ')'                                   # nestedExpr
     | IDENTIFIER '(' fnArgs? ')'                     # functionExpr
     | IDENTIFIER                                     # identifierExpr
     | DOUBLE                                         # doubleExpr
     | LONG                                           # longExpr
     | STRING                                         # string
     ;

fnArgs : expr (',' expr)*                             # functionArgs
       ;

IDENTIFIER : [_$a-zA-Z][_$a-zA-Z0-9]* | '"' (ESC | ~ [\"\\])* '"';
LONG : [0-9]+ ;
DOUBLE : [0-9]+ '.' [0-9]* ;
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
