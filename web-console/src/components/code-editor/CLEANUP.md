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

1. ⬜ 1: the Hjson context from the syntax tree
2. ⬜ 2 + 3: the SQL completions from the syntax tree, with the grammar changes that make that possible (3 removes the
   workaround in 2)
3. ⬜ 6: comments that refer to Ace
4. ⬜ 4: the completion plumbing and the word characters

Section 5 lists what intentionally stays the way it is.

## ⬜ 1. The Hjson context repeats the Hjson grammar

`src/utils/hjson-context.ts` (`getHjsonContext`) is a 405-line state machine that reads the text from the start up to
the cursor. It tracks comments, quoted and multiline strings, brackets and keys, which `hjson.grammar` now gives as
`Object`, `Property`, `PropertyName` and `Array` nodes.

- The JSON path, whether a key or a value is being typed, the current key and the current object can all come from
  walking up the tree from `syntaxTree(state).resolveInner(pos, -1)`.
- **Bug fix:** the scanner never sees the text after the cursor, so a completion rule with a `condition` on a property
  that is written below the cursor (like `type`) does not match. The tree has the whole object.
- `HjsonContext.isEditingComment` is never used.

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

## Not leftovers

These look like they might be, but they are how it would be done in CodeMirror anyway:

- `LineColumn` and `focusEditorAt`: Druid and Hjson report errors as a 1-based line and column.
- `--:ISSUE:`: it comes from `WorkbenchQuery`, not from Ace.
- `stateCacheId` with `EditorState.toJSON`, the compartments, `diffStrings`, the snapshot serializer, the partial query
  markers and the find panel.
