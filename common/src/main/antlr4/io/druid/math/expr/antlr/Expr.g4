grammar Expr;

expr : ('-'|'!') expr                                 # unaryOpExpr
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
     ;

fnArgs : expr (',' expr)*                             # functionArgs
       ;

IDENTIFIER : [_$a-zA-Z][_$a-zA-Z0-9]* ;
LONG : [0-9]+ ;
DOUBLE : [0-9]+ '.' [0-9]* ;
WS : [ \t\r\n]+ -> skip ;

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
