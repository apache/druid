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

// `code`, *emphasis* (which can contain code) or a backslash escaped character
const INLINE_REGEXP = /`([^`]*)`|\*((?:\\.|`[^`]*`|[^*\\`])+)\*|\\(.)/g;

function renderInline(text: string): (Node | string)[] {
  const nodes: (Node | string)[] = [];
  let lastIndex = 0;
  for (const m of text.matchAll(INLINE_REGEXP)) {
    if (m.index > lastIndex) nodes.push(text.slice(lastIndex, m.index));
    const [, code, emphasis, escaped] = m;
    if (typeof code === 'string') {
      const codeElement = document.createElement('code');
      codeElement.textContent = code;
      nodes.push(codeElement);
    } else if (typeof emphasis === 'string') {
      const emphasisElement = document.createElement('em');
      emphasisElement.append(...renderInline(emphasis));
      nodes.push(emphasisElement);
    } else {
      nodes.push(escaped);
    }
    lastIndex = m.index + m[0].length;
  }
  if (lastIndex < text.length) nodes.push(text.slice(lastIndex));
  return nodes;
}

/**
 * Renders "doc markdown", the simplified markdown that the SQL docs in lib/sql-docs.ts are written in (see
 * sanitizeMarkdown in script/create-sql-docs.mjs):
 * - `code` spans
 * - *emphasis*
 * - line breaks (\n)
 * - list items (lines that start with "- ")
 * - a backslash escapes the next character
 */
export function renderDocMarkdown(markdown: string): DocumentFragment {
  const fragment = document.createDocumentFragment();
  let list: HTMLUListElement | undefined;
  let afterText = false;
  for (const line of markdown.split('\n')) {
    if (line.startsWith('- ')) {
      if (!list) {
        list = document.createElement('ul');
        fragment.append(list);
      }
      const item = document.createElement('li');
      item.append(...renderInline(line.slice(2)));
      list.append(item);
      afterText = false;
    } else {
      list = undefined;
      if (afterText) fragment.append(document.createElement('br'));
      fragment.append(...renderInline(line));
      afterText = true;
    }
  }
  return fragment;
}
