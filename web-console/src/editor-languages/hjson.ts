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

import { indentNodeProp, LanguageSupport, LRLanguage } from '@codemirror/language';
import { styleTags, tags } from '@lezer/highlight';

import { makeCompletionSource } from '../components/code-editor/completion-source';
import { getHjsonCompletions } from '../editor-completions/hjson-completions';
import type { JsonCompletionRule } from '../utils';

import { parser } from './hjson.parser';

export const hjsonLanguage = LRLanguage.define({
  name: 'hjson',
  parser: parser.configure({
    props: [
      styleTags({
        // The keys (quoted or not) are styled as a whole, including any escapes
        'PropertyName PropertyName/String PropertyName/String/Escape PropertyName/String/InvalidEscape':
          tags.propertyName,
        'String MultilineString QuotelessString': tags.string,
        'Escape': tags.escape,
        'InvalidEscape': tags.invalid,
        'Number': tags.number,
        // Not styled (tags.null would be, as a keyword)
        'True False': tags.bool,
        'Null': tags.literal,
        'LineComment': tags.lineComment,
        'BlockComment': tags.blockComment,
        '{ }': tags.brace,
        '[ ]': tags.squareBracket,
        ', :': tags.separator,
      }),
      // A new line keeps the indentation of the line before (rather than indenting inside brackets)
      indentNodeProp.add({ 'Document Object Array': () => null }),
    ],
  }),
  languageData: {
    commentTokens: { line: '//', block: { open: '/*', close: '*/' } },
  },
});

const plainHjson = new LanguageSupport(hjsonLanguage);
const hjsonWithCompletions = new WeakMap<JsonCompletionRule[], LanguageSupport>();

export interface HjsonOptions {
  /** The rules for the property and value completions */
  jsonCompletions?: JsonCompletionRule[];
}

/**
 * Hjson support for a CodeEditor: highlighting and, given jsonCompletions, completions. The same options always give
 * the same LanguageSupport so that it can be created while rendering.
 */
export function hjson({ jsonCompletions }: HjsonOptions = {}): LanguageSupport {
  if (!jsonCompletions) return plainHjson;
  let support = hjsonWithCompletions.get(jsonCompletions);
  if (!support) {
    support = new LanguageSupport(
      hjsonLanguage,
      hjsonLanguage.data.of({
        autocomplete: makeCompletionSource((context, word) =>
          getHjsonCompletions(context, word, jsonCompletions),
        ),
      }),
    );
    hjsonWithCompletions.set(jsonCompletions, support);
  }
  return support;
}
