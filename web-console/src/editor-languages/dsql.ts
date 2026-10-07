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

import { closeBrackets, closeBracketsKeymap } from '@codemirror/autocomplete';
import { indentNodeProp, LanguageSupport, LRLanguage } from '@codemirror/language';
import { keymap } from '@codemirror/view';
import { styleTags, tags } from '@lezer/highlight';

import { makeCompletionSource } from '../components/code-editor/completion-source';
import type { SqlCompletionOptions } from '../editor-completions/sql-completions';
import { getSqlCompletions } from '../editor-completions/sql-completions';
import type { AvailableFunctions } from '../helpers';

import { parser } from './dsql.parser';
import { makeIdentifierSpecializer, specializeIdentifier } from './dsql-tokens';
import { editorTags } from './editor-tags';

const dsqlHighlighting = styleTags({
  'Keyword': tags.keyword,
  'FunctionName': tags.function(tags.variableName),
  'Constant': tags.atom,
  'TypeName': tags.typeName,
  'QuotedIdentifier': editorTags.column,
  'String': tags.string,
  'Number': tags.number,
  'Issue': editorTags.issue,
  'LineComment': tags.lineComment,
  'BlockComment': tags.blockComment,
  'Operator': tags.operator,
  '( )': tags.paren,
});

function makeDsqlLanguage(availableSqlFunctions: AvailableFunctions | undefined): LRLanguage {
  return LRLanguage.define({
    name: 'dsql',
    parser: parser.configure({
      props: [
        dsqlHighlighting,
        // Like Ace, a new line keeps the indentation of the line before (rather than indenting inside parentheses)
        indentNodeProp.add({ 'Script Parens': () => null }),
      ],
      specializers: availableSqlFunctions
        ? [{ from: specializeIdentifier, to: makeIdentifierSpecializer(availableSqlFunctions) }]
        : [],
    }),
    languageData: {
      commentTokens: { line: '--' },
      // Like Ace, do not auto close braces (they are rare in SQL and typing one usually means the start of a JSON query)
      closeBrackets: { brackets: ['(', '[', "'", '"'] },
    },
  });
}

const defaultDsqlLanguage = makeDsqlLanguage(undefined);
const dsqlLanguages = new WeakMap<AvailableFunctions, LRLanguage>();

/**
 * The DruidSQL language. The functions that the cluster has (in addition to the documented ones) are highlighted as
 * functions. The same functions always give the same language so that reconfiguring an editor does not re-parse it.
 */
export function getDsqlLanguage(availableSqlFunctions?: AvailableFunctions): LRLanguage {
  if (!availableSqlFunctions) return defaultDsqlLanguage;
  let language = dsqlLanguages.get(availableSqlFunctions);
  if (!language) {
    language = makeDsqlLanguage(availableSqlFunctions);
    dsqlLanguages.set(availableSqlFunctions, language);
  }
  return language;
}

export type DsqlOptions = SqlCompletionOptions;

/**
 * DruidSQL support for a CodeEditor: highlighting, completions and bracket closing
 */
export function dsql(options: DsqlOptions = {}): LanguageSupport {
  const language = getDsqlLanguage(options.availableSqlFunctions);
  return new LanguageSupport(language, [
    language.data.of({
      autocomplete: makeCompletionSource((context, word) =>
        getSqlCompletions(context, word, options),
      ),
    }),
    closeBrackets(),
    keymap.of(closeBracketsKeymap),
  ]);
}
