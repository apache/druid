<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

# Cleanup: code that would not be there if the console had used CodeMirror from day 1

The editor used to be Ace. These are the places where code is still shaped by that, and what it would look like if it
had been written for CodeMirror in the first place.

Status: ✅ done, ⬜ to do.

## Suggested order

1. ✅ 1: the Hjson context from the syntax tree
2. ⬜ 2 + 3: the SQL completions from the syntax tree, with the grammar changes that make that possible (3 removes the
   workaround in 2)
3. ⬜ 6: comments that refer to Ace
4. ⬜ 4: the completion plumbing and the word characters
5. ✅ 7: underline the Hjson parse error in the editor

Section 5 lists what intentionally stays the way it is.

## ✅ 1. The Hjson context from the syntax tree

`src/utils/hjson-context.ts` was a 405-line state machine that read the text from the start up to the cursor, tracking
comments, quoted and multiline strings, brackets and keys, which `hjson.grammar` gives as `Object`, `Property`,
`PropertyName` and `Array` nodes. It is replaced by `src/editor-completions/hjson-context.ts`:

- `getHjsonContext(state, pos)` walks up the tree from `resolveInner(pos, -1)` to the object or array that the cursor
  is in, and builds the JSON path from the `Property` names and `Array` indexes above it.
- A value is being edited when the cursor is after the colon of the property it is in (or of the property right before
  it, when nothing is typed yet), up to the end of the value. A value that starts after the cursor is on a line below
  (the error recovery took the next line as the value) and also counts as the value being typed.
- `currentObject` is built from the object's `Property` nodes, without the property that is being edited. Strings are
  read with `JSON.parse` and multiline strings with `Hjson.parse` (for the indentation).
- **Bug fix:** the object now includes the properties after the cursor, so a completion rule with a `condition` on a
  property written below the cursor (like `queryType`) matches, and keys written below are not suggested again.
- `currentKey` is only set when editing a value (it was sometimes the half-typed key, which nothing used), and
  `isEditingComment` (never used) is gone. The comment check stays in `getHjsonCompletions`.

## ⬜ 2. The SQL completions read the text with regexes

In `src/editor-completions/sql-completions.ts`:

- The keyword before the word being typed is found with a regex on the current line only, so a keyword on the line
  above is ignored (`GROUP` on one line and the cursor on the next does not narrow the suggestions to `BY`). Looking
  back through the tree, skipping comments, would fix this.
- `getPossibleSqlReferences` and `getSqlLiterals` scan the text with regexes, and `KNOWN_SQL_PARTS` repeats the word
  list that `dsql-tokens.ts` already has. Collecting the `Identifier`, `QuotedIdentifier` and `String` nodes would
  replace all three. An `Identifier` node already leaves out the known words.
- The `charBeforePrefix === "'"` check only exists because an unclosed literal is not a `String` node (see 3).

## ⬜ 3. Grammar choices copied from Ace's regexes

These were made so that the highlighting stayed the same as Ace's:

- In `dsql.grammar`, an unclosed quote (`'` or `"`) is a `Punctuation` character. A grammar written from scratch would
  make it a `String` (or `QuotedIdentifier`) that runs to the end of the line, so it is colored while it is typed and
  the workaround in 2 can go. **This changes what users see.**
- In `dsql.grammar`, the sign is part of the number, so `a-1` is `a` and `-1`. Normally `-` would be an `Operator`.
- In `hjson.grammar`, quoted strings can span lines "like in Ace".

## ⬜ 4. Completion plumbing shaped like Ace's completer API

- `EditorCompletion` and `CompletionDoc` (`editor-completion.ts`) copy CodeMirror's `Completion`.
  `EditorCompletionBuilder`, `CompletionWord` and `makeCompletionSource` (`completion-source.ts`) wrap what would
  normally be a plain `CompletionSource` per language. Keeping the builders free of DOM code (and easy to test) is
  still worth something, so this is a judgment call.
- `PREFIX_REGEXP` in `completion-source.ts` is Ace's `ID_REGEX`, the same for every language. It includes `-`, so `a-b`
  is one word in SQL. In CodeMirror, word characters are language data (`wordChars`) or the source's own `matchBefore`.
  **Decide first:** this changes which part of the text is completed (for example `foo-ba`), so it is a behavior
  change.

## 5. Intentional: behavior that stays like Ace

Not action items. This code exists to behave or look like Ace did, and that behavior is to be kept:

- `insertSoftTab` in `code-editor.tsx`: with nothing selected, Tab inserts spaces up to the next tab stop.
- `positionInfo` in `code-editor.tsx`: the doc panel sits next to the completion list, aligned with its top.
- A new line keeps the indentation of the line before (`indentNodeProp` returns `null` in `dsql.ts` and `hjson.ts`).
- `closeBrackets` in `dsql.ts` does not close `{` (typing one usually starts a native JSON query).
- The theme (`code-editor-theme.ts`) follows Ace's solarized_dark with the console's overrides, Ace's font stack and
  4px line padding.
- `overflow: hidden` in `code-editor.scss`: the editor never grows past the size it is given.
- The shared tooltip host prepended to the body: tooltips are not clipped by the editor's containers, and
  `document.body.lastChild` stays the Blueprint portal that the dialog specs snapshot.

## ⬜ 6. Comments that refer to Ace

About ten comments explain a choice with "Like Ace…" (`code-editor.tsx`, `code-editor-theme.ts`, `code-editor.scss`,
`dsql.ts`, `hjson.ts`, `hjson.grammar`). They should give the actual reason instead, for example "a new line keeps the
indentation of the line before".

## ✅ 7. Underline the Hjson parse error in the editor

A new feature rather than a leftover. Before, a parse error was only shown as text: under the editor in `JsonInput`
(once it loses focus), and as a toast when a JSON query is run in the workbench.

- `error-mark.ts`: a `StateField` of one mark decoration (our own, not `@codemirror/lint`), set with
  `showEditorError(view, { position, message } | undefined)` and cleared on the next change to the text. It is part
  of every `CodeEditor`.
- The mark is a wavy underline (`.cm-errorMark`, in the color of the issue comments) with the message in a
  `data-tooltip` attribute, so the console's mouse tooltip shows it on hover.
- It marks the syntax node at the position when it is on one line (`Hjson.parse` gives a position, not a range),
  otherwise the rest of the line, or the last character before the position at the end of a line.
- `Hjson.parse` stays the parser (see "Considered and rejected" below). `getHjsonEditorError` (`json-input.tsx`) turns
  its message into the position and the message without `at line …` and the `>>>` excerpt.
- `JsonInput` marks the error when the message under the editor shows up. The workbench marks the issue of a JSON
  query when it is run, next to the toast.
- Later, the same mark could show Druid's SQL errors, since `DruidError` has `startLineColumn` and `endLineColumn`.

## Considered and rejected

- **Parsing Hjson with `hjson.grammar` instead of the `hjson` package.** The grammar is forgiving on purpose (a key
  before its colon is typed, unclosed strings and comments, catch-all characters), Lezer gives an error position but
  no message (and the line and column are read back out of the message), and building the value means redoing
  Hjson's rules (escapes, `'''` indentation, quoteless trimming, numbers, duplicate keys, the root without braces) in
  the code that builds what is sent to Druid. The package only saves about 13 KB of parse code.

## Not leftovers

These look like they might be, but they are how it would be done in CodeMirror anyway:

- `LineColumn` and `focusEditorAt`: Druid and Hjson report errors as a 1-based line and column.
- `--:ISSUE:`: it comes from `WorkbenchQuery`, not from Ace.
- `stateCacheId` with `EditorState.toJSON`, the compartments, `diffStrings`, the snapshot serializer, the partial query
  markers and the find panel.
