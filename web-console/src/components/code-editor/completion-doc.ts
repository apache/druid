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

import type { CompletionDoc } from '../../editor-completions/editor-completion';

import { renderDocMarkdown } from './doc-markdown';

function makeDiv(className: string): HTMLDivElement {
  const div = document.createElement('div');
  div.className = className;
  return div;
}

/**
 * Renders the documentation panel that is shown next to the completion list (styled in code-editor-theme.ts)
 */
export function renderCompletionDoc({
  name,
  syntax,
  description,
  descriptionMarkdown,
}: CompletionDoc): HTMLElement {
  const container = document.createElement('div');

  const nameDiv = makeDiv('doc-name');
  nameDiv.textContent = name;
  container.append(nameDiv);

  if (syntax) {
    const syntaxDiv = makeDiv('doc-syntax');
    syntaxDiv.textContent = syntax;
    container.append(syntaxDiv);
  }

  if (descriptionMarkdown || description) {
    const descriptionDiv = makeDiv('doc-description');
    if (descriptionMarkdown) {
      descriptionDiv.append(renderDocMarkdown(descriptionMarkdown));
    } else {
      descriptionDiv.textContent = description!;
    }
    container.append(descriptionDiv);
  }

  return container;
}
