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

import awesomeCodeStyle, { configs } from '@awesome-code-style/eslint-config';
import awesomeCodeStyleReact, { reactConfigs } from '@awesome-code-style/eslint-config/react';
import notice from 'eslint-plugin-notice';
import regexp from 'eslint-plugin-regexp';
import globals from 'globals';

const TYPESCRIPT_FILES = ['**/*.ts', '**/*.tsx'];

export default [
  {
    // The parsers generated from the grammars by script/build-grammars.mjs
    ignores: [
      'public',
      'target',
      'src/editor-languages/*.parser.ts',
      'src/editor-languages/*.parser.terms.ts',
    ],
  },
  ...awesomeCodeStyle,
  ...awesomeCodeStyleReact,
  ...configs.typeChecked.map(config => ({ ...config, files: TYPESCRIPT_FILES })),
  ...reactConfigs.reactTypeChecked.map(config => ({ ...config, files: TYPESCRIPT_FILES })),
  {
    files: TYPESCRIPT_FILES,
    rules: {
      '@typescript-eslint/no-deprecated': [2],
    },
  },
  {
    plugins: {
      notice,
    },
    rules: {
      'notice/notice': [2, { mustMatch: 'Licensed to the Apache Software Foundation \\(ASF\\).+' }],
      'react/jsx-no-bind': [2, { allowArrowFunctions: true, allowFunctions: true }],
      '@typescript-eslint/switch-exhaustiveness-check': [0], // ToDo: `considerDefaultExhaustiveForUnions: true` should be set upstream on awesome-code-style, then this rule can be re-enabled
    },
  },
  {
    // Local versions of the CodeQL checks run on the PRs
    plugins: {
      regexp,
    },
    rules: {
      // CodeQL: Inefficient regular expression / Polynomial regular expression used on uncontrolled data
      'regexp/no-super-linear-backtracking': [2],
      'regexp/no-super-linear-move': [2],
      // CodeQL: Incomplete string escaping or encoding (a string pattern only replaces the first occurrence)
      'no-restricted-syntax': [
        2,
        {
          selector:
            "CallExpression[callee.property.name='replace'][arguments.0.value=/^[\\\\'\"`<>&|]$/]",
          message:
            'Replacing a string only replaces the first occurrence, use replaceAll or a regular expression (with the g flag if all occurrences should be replaced)',
        },
      ],
    },
  },
  {
    files: ['*.js', 'lib/*.js', 'script/*.js'],
    languageOptions: {
      globals: globals.node,
    },
    rules: {
      '@typescript-eslint/no-require-imports': [0],
    },
  },
  {
    files: ['e2e-tests/**/*.ts'],
    rules: {
      '@typescript-eslint/no-unsafe-declaration-merging': [0],
    },
  },
];
