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

import { renderDocMarkdown } from './doc-markdown';

function renderToHtml(markdown: string): string {
  const div = document.createElement('div');
  div.append(renderDocMarkdown(markdown));
  return div.innerHTML;
}

describe('renderDocMarkdown', () => {
  it('renders plain text', () => {
    expect(renderToHtml('Reads external data.')).toEqual('Reads external data.');
  });

  it('renders code and emphasis', () => {
    expect(renderToHtml('*Deprecated.* Use `APPROX_QUANTILE_DS` instead.')).toEqual(
      '<em>Deprecated.</em> Use <code>APPROX_QUANTILE_DS</code> instead.',
    );
    expect(renderToHtml('*Use `X`* now')).toEqual('<em>Use <code>X</code></em> now');
  });

  it('keeps the content of code as text', () => {
    expect(renderToHtml('Returns `ARRAY<COMPLEX<json>>` or `a * b`')).toEqual(
      'Returns <code>ARRAY&lt;COMPLEX&lt;json&gt;&gt;</code> or <code>a * b</code>',
    );
  });

  it('renders escaped characters as text', () => {
    expect(renderToHtml('polar \\* coordinates, \\`, \\\\ and <b>')).toEqual(
      'polar * coordinates, `, \\ and &lt;b&gt;',
    );
    expect(renderToHtml('\\- not a list item')).toEqual('- not a list item');
  });

  it('renders line breaks', () => {
    expect(renderToHtml('First.\n\nSecond.')).toEqual('First.<br><br>Second.');
  });

  it('renders lists', () => {
    expect(renderToHtml('Extensions:\n- `a`: one\n- `b`: two\nAfter')).toEqual(
      'Extensions:<ul><li><code>a</code>: one</li><li><code>b</code>: two</li></ul>After',
    );
  });
});
