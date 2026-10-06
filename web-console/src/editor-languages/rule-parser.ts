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

import type { StreamParser } from '@codemirror/language';
import { Tag, tags } from '@lezer/highlight';

/**
 * Tags for the tokens that are styled in a way that does not fit any of the standard tags
 */
export const editorTags = {
  issue: Tag.define(tags.comment),
  column: Tag.define(tags.variableName),
};

/**
 * The token names that the modes produce and the tags they are highlighted with
 */
export const TOKEN_TABLE: Record<string, Tag> = {
  keyword: tags.keyword,
  function: tags.function(tags.variableName),
  constant: tags.atom,
  typeName: tags.typeName,
  number: tags.number,
  string: tags.string,
  escape: tags.escape,
  literal: tags.literal,
  comment: tags.comment,
  issue: editorTags.issue,
  column: editorTags.column,
  operator: tags.operator,
  paren: tags.paren,
  invalid: tags.invalid,
};

export interface TokenRule {
  regex: RegExp;
  token: string | null | ((value: string) => string | null);
  push?: string;
  pop?: boolean;
}

interface RuleParserState {
  stack: string[];
}

/**
 * Creates a stream parser from a set of rules in the style of Ace's highlight rules: in every state the rules are tried
 * in order and the first one that matches at the current position produces the token. A rule can push a new state onto
 * the stack (a zero width rule can only do that) or pop the current state off of it. Characters that no rule matches
 * are not highlighted. The rules are matched against the whole line so that `\b` and lookbehinds see the
 * text before the current position.
 */
export function createRuleParser(
  rules: Record<string, TokenRule[]>,
): Pick<StreamParser<RuleParserState>, 'startState' | 'copyState' | 'token'> {
  const stickyRules: Record<string, TokenRule[]> = {};
  for (const [stateName, stateRules] of Object.entries(rules)) {
    stickyRules[stateName] = stateRules.map(rule => ({
      ...rule,
      regex: new RegExp(rule.regex.source, rule.regex.flags.replace(/[gy]/g, '') + 'y'),
    }));
  }

  return {
    startState: () => ({ stack: ['start'] }),
    copyState: state => ({ stack: state.stack.slice() }),
    token(stream, state) {
      // Zero width rules change the state without consuming anything, the limit guards against rules that loop
      for (let attempt = 0; attempt < 10; attempt++) {
        const stateName = state.stack[state.stack.length - 1];
        let stateChanged = false;
        for (const rule of stickyRules[stateName] || []) {
          rule.regex.lastIndex = stream.pos;
          const m = rule.regex.exec(stream.string);
          if (!m) continue;
          if (!m[0].length && !rule.push && !rule.pop) continue;

          if (rule.pop) {
            if (state.stack.length > 1) state.stack.pop();
          } else if (rule.push) {
            state.stack.push(rule.push);
          }

          if (!m[0].length) {
            stateChanged = true;
            break;
          }

          stream.pos += m[0].length;
          return typeof rule.token === 'function' ? rule.token(m[0]) : rule.token;
        }
        if (!stateChanged) break;
      }

      stream.next();
      return null;
    },
  };
}
