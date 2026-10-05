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

/**
 * Documentation shown next to the completion list when the completion is selected
 */
export interface CompletionDoc {
  /** The title */
  name: string;
  /** Plain text shown (in monospace) under the title, like a function signature */
  syntax?: string;
  /** Plain text description */
  description?: string;
  /** HTML description, only for trusted HTML like the docs in lib/sql-docs.ts */
  descriptionHtml?: string;
}

/**
 * A completion suggestion. The fields mirror the ones of CodeMirror's `Completion`.
 */
export interface EditorCompletion {
  /** The text that is inserted (and matched against what was typed) */
  label: string;
  /** What is shown in the list, defaults to the label */
  displayLabel?: string;
  /** Shown to the right of the label, like 'column' or 'function' */
  detail?: string;
  /** Ranks equally good matches, from -99 to 99, higher first */
  boost?: number;
  doc?: CompletionDoc;
}
