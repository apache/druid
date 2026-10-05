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

import { renderCompletionDoc } from './completion-doc';

describe('renderCompletionDoc', () => {
  it('renders the name, syntax and HTML description', () => {
    expect(
      renderCompletionDoc({
        name: 'COUNT',
        syntax: 'COUNT(*)',
        descriptionHtml: 'Counts the number of <code>things</code>',
      }).innerHTML,
    ).toEqual(
      '<div class="doc-name">COUNT</div>' +
        '<div class="doc-syntax">COUNT(*)</div>' +
        '<div class="doc-description">Counts the number of <code>things</code></div>',
    );
  });

  it('escapes the plain text parts', () => {
    expect(
      renderCompletionDoc({
        name: 'ARRAY<STRING>',
        syntax: 'f(a < b)',
        description: 'Type of COMPLEX<json>',
      }).innerHTML,
    ).toEqual(
      '<div class="doc-name">ARRAY&lt;STRING&gt;</div>' +
        '<div class="doc-syntax">f(a &lt; b)</div>' +
        '<div class="doc-description">Type of COMPLEX&lt;json&gt;</div>',
    );
  });

  it('leaves out the empty parts', () => {
    expect(renderCompletionDoc({ name: 'REAL', descriptionHtml: '' }).innerHTML).toEqual(
      '<div class="doc-name">REAL</div>',
    );
  });
});
