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

import type { InputStream } from '@lezer/lr';
import { ExternalTokenizer } from '@lezer/lr';

import { quotelessKey, QuotelessString } from './hjson.parser.terms';

const TAB = 9;
const NEWLINE = 10;
const CARRIAGE_RETURN = 13;
const SPACE = 32;
const DOUBLE_QUOTE = 34;
const HASH = 35;
const SINGLE_QUOTE = 39;
const STAR = 42;
const MINUS = 45;
const DOT = 46;
const SLASH = 47;
const ZERO = 48;
const NINE = 57;
const COLON = 58;
const UPPER_E = 69;
const LOWER_E = 101;
const PLUS = 43;

// The characters that end a quoteless key and that a quoteless string can not start with
const PUNCTUATORS = new Set(Array.from(',:[]{}', c => c.charCodeAt(0)));

// The characters that can follow a number or a literal (true, false, null) that is not part of a quoteless string
const VALUE_ENDS = new Set(Array.from(',]}', c => c.charCodeAt(0)));

const LITERALS = ['true', 'false', 'null'];

function isInlineSpace(c: number): boolean {
  return c === SPACE || c === TAB;
}

function isSpace(c: number): boolean {
  return isInlineSpace(c) || c === NEWLINE || c === CARRIAGE_RETURN;
}

function isLineEnd(c: number): boolean {
  return c < 0 || c === NEWLINE || c === CARRIAGE_RETURN;
}

function isDigit(c: number): boolean {
  return c >= ZERO && c <= NINE;
}

function startsComment(input: InputStream, offset: number): boolean {
  const c = input.peek(offset);
  if (c === HASH) return true;
  const next = input.peek(offset + 1);
  return c === SLASH && (next === SLASH || next === STAR);
}

function startsMultilineString(input: InputStream): boolean {
  return (
    input.peek(0) === SINGLE_QUOTE &&
    input.peek(1) === SINGLE_QUOTE &&
    input.peek(2) === SINGLE_QUOTE
  );
}

function skipDigits(input: InputStream, offset: number): number {
  while (isDigit(input.peek(offset))) offset++;
  return offset;
}

/**
 * The length of the number (as the grammar's Number token matches it) or the literal at the current position, 0 if
 * there is none
 */
function numberOrLiteralLength(input: InputStream): number {
  for (const literal of LITERALS) {
    if (Array.from(literal).every((c, i) => input.peek(i) === c.charCodeAt(0))) {
      return literal.length;
    }
  }

  let offset = input.peek(0) === MINUS ? 1 : 0;
  const first = input.peek(offset);
  if (first === ZERO) {
    offset++;
  } else if (isDigit(first)) {
    offset = skipDigits(input, offset);
  } else {
    return 0;
  }

  if (input.peek(offset) === DOT && isDigit(input.peek(offset + 1))) {
    offset = skipDigits(input, offset + 1);
  }

  const e = input.peek(offset);
  if (e === LOWER_E || e === UPPER_E) {
    let exponent = offset + 1;
    const sign = input.peek(exponent);
    if (sign === PLUS || sign === MINUS) exponent++;
    if (isDigit(input.peek(exponent))) offset = skipDigits(input, exponent);
  }

  return offset;
}

/**
 * Whether the text at the given offset ends a number or a literal: the end of the line, a comma, a closing bracket or a
 * comment (possibly after some spaces)
 */
function endsValue(input: InputStream, offset: number): boolean {
  while (isInlineSpace(input.peek(offset))) offset++;
  const c = input.peek(offset);
  return isLineEnd(c) || VALUE_ENDS.has(c) || startsComment(input, offset);
}

/**
 * Produces the tokens of Hjson that depend on the context:
 *
 * - quotelessKey: where a property can start, a word that is followed by a colon on the same line (or any word inside
 *   an object, where a value can not start)
 * - QuotelessString: where a value can start, the rest of the line, unless it is just a number or a literal (`1`,
 *   `true, ...`), those are left to the grammar's tokens. A quoteless string can not start with a punctuator (`,:[]{}`),
 *   a double quote or a comment, but it can contain them.
 */
export const quoteless = new ExternalTokenizer(
  (input, stack) => {
    const first = input.next;
    if (
      first < 0 ||
      isSpace(first) ||
      PUNCTUATORS.has(first) ||
      first === DOUBLE_QUOTE ||
      startsComment(input, 0) ||
      startsMultilineString(input)
    ) {
      return;
    }

    if (stack.canShift(quotelessKey)) {
      let keyEnd = 0;
      while (
        !isSpace(input.peek(keyEnd)) &&
        !PUNCTUATORS.has(input.peek(keyEnd)) &&
        input.peek(keyEnd) >= 0
      ) {
        keyEnd++;
      }
      let colon = keyEnd;
      while (isInlineSpace(input.peek(colon))) colon++;
      // Inside an object (where no value can start) a word is a key even before its colon is typed
      if (input.peek(colon) === COLON || (keyEnd && !stack.canShift(QuotelessString))) {
        input.acceptToken(quotelessKey, keyEnd);
        return;
      }
    }

    if (stack.canShift(QuotelessString)) {
      const valueLength = numberOrLiteralLength(input);
      if (valueLength && endsValue(input, valueLength)) return;

      let end = 0;
      while (!isLineEnd(input.peek(end))) end++;
      input.acceptToken(QuotelessString, end);
    }
  },
  { contextual: true },
);
