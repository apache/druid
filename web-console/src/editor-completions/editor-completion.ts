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

import type { Completion } from '@codemirror/autocomplete';

/**
 * Documentation shown next to the completion list when the completion is selected
 */
export interface CompletionDoc {
  /** The title */
  name: string;
  /** Plain text shown (in monospace) under the title, like a function signature (one per line if there are several) */
  syntax?: string;
  /** Plain text description */
  description?: string;
  /** Description in "doc markdown", the simplified markdown of the docs in lib/sql-docs.ts */
  descriptionMarkdown?: string;
}

/**
 * A completion suggestion: CodeMirror's `Completion` (`label`, `detail`, `boost`, ...) with the documentation as data
 * (`doc`) rather than an `info` that renders it. `makeCompletionSource` turns the `doc` into the `info`.
 */
export type EditorCompletion = Omit<Completion, 'info'> & { doc?: CompletionDoc };
