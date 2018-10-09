/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
