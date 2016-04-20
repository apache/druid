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
     | CASE expr ( WHEN expr THEN expr )+ ( ELSE expr )? END    # caseWhenExpr
     ;

fnArgs : expr (',' expr)*                             # functionArgs
       ;

CASE : C A S E;
WHEN : W H E N;
THEN : T H E N;
ELSE : E L S E;
END  : E N D;

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

fragment A:('a'|'A');
fragment B:('b'|'B');
fragment C:('c'|'C');
fragment D:('d'|'D');
fragment E:('e'|'E');
fragment F:('f'|'F');
fragment G:('g'|'G');
fragment H:('h'|'H');
fragment I:('i'|'I');
fragment J:('j'|'J');
fragment K:('k'|'K');
fragment L:('l'|'L');
fragment M:('m'|'M');
fragment N:('n'|'N');
fragment O:('o'|'O');
fragment P:('p'|'P');
fragment Q:('q'|'Q');
fragment R:('r'|'R');
fragment S:('s'|'S');
fragment T:('t'|'T');
fragment U:('u'|'U');
fragment V:('v'|'V');
fragment W:('w'|'W');
fragment X:('x'|'X');
fragment Y:('y'|'Y');
fragment Z:('z'|'Z');
