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

import type { Ace } from 'ace-builds';

// Removes noise from DOM snapshots so that they stay small and a change shows up as a small diff:
// - the empty padding rows that ConsoleTable always renders are collapsed into a single comment
// - icon <svg>s are reduced to their icon name (the path data changes whenever an icon is redrawn)
// - the inside of Ace editors (Ace's own DOM, which changes with every Ace version) is replaced with a comment
//   describing what the console configured: the mode, the value and the placeholder

const cleaned = new WeakSet<Node>();

function describeAceEditor(editor: Ace.Editor): string {
  const parts = [`mode: ${(editor.session.getMode() as any).$id}`];
  parts.push(`value: ${JSON.stringify(editor.getValue())}`);
  const placeholder: string | undefined = (editor.renderer as any).placeholderNode?.textContent;
  if (placeholder) parts.push(`placeholder: ${JSON.stringify(placeholder)}`);
  if (editor.getReadOnly()) parts.push('read only');
  return ` Ace editor, ${parts.join(', ')} `;
}

function simplifyDom(root: Element, original: Element): void {
  const originalEditors = Array.from(original.querySelectorAll('.ace_editor'));
  Array.from(root.querySelectorAll('.ace_editor')).forEach((editorElement, i) => {
    const editor: Ace.Editor | undefined = (originalEditors[i] as any)?.env?.editor;
    if (!editor) return;
    editorElement.replaceChildren(
      editorElement.ownerDocument.createComment(describeAceEditor(editor)),
    );
  });

  for (const tbody of Array.from(root.querySelectorAll('.ct-tbody'))) {
    const padRowGroups = Array.from(tbody.children).filter(group =>
      group.querySelector(':scope > .ct-tr.-padRow'),
    );
    if (!padRowGroups.length) continue;
    tbody.insertBefore(
      tbody.ownerDocument.createComment(` ${padRowGroups.length} empty rows `),
      padRowGroups[0],
    );
    for (const group of padRowGroups) group.remove();
  }

  for (const svg of Array.from(root.querySelectorAll('svg[data-icon]'))) {
    const icon = svg.ownerDocument.createElementNS('http://www.w3.org/2000/svg', 'svg');
    icon.setAttribute('data-icon', svg.getAttribute('data-icon')!);
    svg.replaceWith(icon);
  }
}

export const domSnapshotSerializer: jest.SnapshotSerializerPlugin = {
  test(value: unknown) {
    return value instanceof Element && !cleaned.has(value);
  },

  serialize(value: Element, config, indentation, depth, refs, printer) {
    const clone = value.cloneNode(true) as Element;
    simplifyDom(clone, value);
    cleaned.add(clone);
    for (const element of Array.from(clone.querySelectorAll('*'))) cleaned.add(element);
    return printer(clone, config, indentation, depth, refs);
  },
};
