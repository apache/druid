/** Based on v1.4 from their docs
 at https://docs.influxdata.com/influxdb/v1.4/write_protocols/line_protocol_tutorial/
 **/

grammar InfluxLineProtocol;

lines
    : line ('\n' line)* '\n'? EOF
;

line
    : identifier (',' tag_set)? ' ' field_set (' ' timestamp)?
;

timestamp
    : NUMBER
;

field_set
    : field_pair (',' field_pair)*
;

tag_set
    : tag_pair (',' tag_pair)*
;

tag_pair
    : identifier '=' identifier
;

field_pair
    : identifier '=' field_value
;

identifier
    : IDENTIFIER_STRING | NUMBER | BOOLEAN
;

field_value
    : QUOTED_STRING | NUMBER | BOOLEAN
;

eol
    : NEWLINE | EOF
;

NEWLINE
    : '\n'
;

NUMBER
    : '-'? INT ('.' [0-9] +) ? 'i'?
;

BOOLEAN
    : 'TRUE' | 'true' | 'True' | 't' | 'T' | 'FALSE' | 'False' | 'false' | 'F' | 'f'
;

QUOTED_STRING
    : '"' (StringFieldEscapeSequence | ~(["\\]) )* '"'
;

IDENTIFIER_STRING
    : (IdentifierEscapeSequence | ~([,= \n\\]) )+
;

fragment IdentifierEscapeSequence
    : '\\' [,= \\]
;

fragment StringFieldEscapeSequence
    : '\\' ["\\]
;

fragment INT
   : '0' | [1-9] [0-9]*
;
