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

# Plan: make the code editor code idiomatic CodeMirror

The [migration from Ace](./MIGRATION.md) deliberately kept a lot of Ace-shaped code outside of `CodeEditor` so that
the changeset stayed small. This plan lists the Ace habits that are left, so that the code reads naturally to someone
(or some agent) who knows CodeMirror but has never seen Ace.

**Constraint for every step: the rendering stays as it is.** Only the APIs and the code shape change. The accepted
exceptions are bug fixes (see 1a and 2c).

Status: ✅ done, ⬜ to do.

## Suggested order

1. ✅ 1a + 1b: a CodeMirror-shaped completion type with a structured `doc` (and 1a+: no HTML in the SQL docs)
2. ✅ 1e, 1h: small cleanups
3. ✅ 2a + 2b + 1g: `LanguageSupport` factories that bring their own completion sources
4. ✅ 1c + 1d: completion sources that read the syntax tree
5. ✅ 4a + 4b: the partial query markers
6. ✅ 3a + 3b and 5: positions and prop renames (these touch the most callers)
7. ✅ 6: low priority leftovers

Update [README.md](./README.md) and [MIGRATION.md](./MIGRATION.md) as each step lands.

## 1. Completions

### ✅ 1a. Structured documentation instead of `docHTML`

**Was:** completion builders called `makeDocHtml` to build an HTML string (`docHTML`, an Ace API), which `CodeEditor`
put into the page with `innerHTML`. Two different kinds of text went in the same way: the SQL descriptions in
`lib/sql-docs.ts` are real HTML, but the Hjson `documentation` strings are plain text, and the `name` was not escaped
either.

**Done:**

- `EditorCompletion.doc` is a `CompletionDoc`: `{ name, syntax?, description?, descriptionMarkdown? }`. The type says
  which description is plain text. The SQL builder uses `descriptionMarkdown` (see 1a+), the Hjson builder uses
  `description`.
- `CodeEditor` renders the panel itself with `renderCompletionDoc` (`completion-doc.ts`), which builds the same
  `doc-name` / `doc-syntax` / `doc-description` elements that the theme styles. Nothing is inserted as HTML.
- `src/editor-completions/make-doc-html.ts`, its spec and its snapshot were removed. `completion-doc.spec.ts` replaces
  them.

**Visible change (a bug fix):** Hjson values like `ARRAY<STRING>` and `COMPLEX<json>` used to show up in the doc panel
as `ARRAY` and `COMPLEX` because they were parsed as HTML. They now show up in full.

### ✅ 1a+. No HTML strings in the SQL docs

**Was:** `script/create-sql-docs.mjs` converted the markdown in the Druid docs to HTML with snarkdown, so
`lib/sql-docs.ts` held HTML strings that the doc panel inserted with `innerHTML`.

**Done:**

- The script now produces "doc markdown": the docs' markdown reduced to `` `code` ``, `*emphasis*`, line breaks
  (`\n`), list items (lines starting with `- `) and backslash escapes. Links become their text, `<br>` becomes a line
  break, the one `<ul>` list becomes list items, a few HTML entities are decoded, and any other HTML fails the build.
- `renderDocMarkdown` (`doc-markdown.ts`) turns doc markdown into DOM elements (`code`, `em`, `br`, `ul`/`li`) when the
  panel is shown. `CompletionDoc.descriptionHtml` became `descriptionMarkdown`.
- snarkdown was removed from the dev dependencies.

**Checked:** every SQL doc was rendered both ways. 208 of 219 give exactly the same DOM. The other 11 are fixes of
snarkdown quirks: a single `<br>` in the docs is now a line break (snarkdown turned it into a space, or merged two of
them into one), and a literal `*` in the ATAN2 doc no longer starts italics.

### ✅ 1b. CodeMirror field names for completions

**Was:** `EditorCompletion` was Ace's `ValueCompletion` (`value`, `caption`, `meta`, `score`), and `toCompletion`
translated every field to CodeMirror's `Completion`, clamping `score` to CodeMirror's ±99 `boost` range.

**Done:** the fields are now `label`, `displayLabel`, `detail` and `boost`, the same names as CodeMirror's
`Completion`. `toCompletion` only attaches the doc panel. The clamp was dropped since all boosts are between 1 and 50.
The SQL and Hjson builders and their specs were updated.

**Considered, not done:** CodeMirror's `type` is the field meant for the kind of thing ('keyword', 'function', …). The
console shows that kind as text on the right, which is what `detail` does, so `detail` was kept. With `icons: false`,
`type` would only add a `cm-completionIcon-<type>` class.

### ✅ 1c. Replace `CompletionRequest` with CodeMirror's `CompletionContext`

**Was:** `CompletionRequest` (`prefix`, `charBeforePrefix`, `lineBeforePrefix`, `textBeforePrefix`, `allText`) was how
Ace split up the text around the cursor. `getHjsonCompletions` glued the pieces back together
(`textBefore + charBeforePrefix + prefix`), and `allText` called `doc.toString()` on every request, needed or not.

**Done:**

- The builders take CodeMirror's `CompletionContext` and a `CompletionWord` (`{ from, text }`, the word being
  completed): `getSqlCompletions(context, word, options)` and `getHjsonCompletions(context, word, jsonCompletions)`.
  They read the text they need from `context.state`. `CompletionRequest` is gone.
- `makeCompletionSource(builder)` still owns the parts every source shares (finding the word, skipping read-only
  editors, rendering the doc panel), so the builders stay `EditorCompletion` producers rather than full
  `CompletionSource`s. That keeps the structured `doc` (1a) and keeps the specs readable.
- The specs are written as text with a `|` for the cursor, using `completionContextAt` (`src/test-utils/`). Before,
  they passed the pieces by hand, sometimes in combinations the editor can't produce (a `lineBeforePrefix` that does not
  match `allText`).

**Kept on purpose:** the SQL builder still takes "the keyword before the word" from the line before the word,
excluding the one character right before it, as Ace did. So `x AS "a` still sees `AS`, and `COUNT(a` sees `COUNT`.
Looking past the punctuation another way would change which suggestions show up.

### ✅ 1d. Detect the context with the syntax tree, not regexes

**Was:** the builders looked at the raw text: `sql-completions.ts` treated `--` anywhere on the line as the start of a
comment (even inside `'a -- b'`), did not know about `/* */` comments, and only knew it was inside a string literal
when `'` was right before the word. The Hjson builder found comments by parsing all of the text before the cursor.

**Done:** `tokenBefore(state, pos)` (`completion-source.ts`) reads the token at the cursor from the syntax tree
(`syntaxTree(state).resolveInner(pos, -1)`).

- SQL: no completions in `comment` and `issue` tokens; only literals inside a `string` token.
- Hjson: no completions in `comment` tokens. `getHjsonContext` is still used for the JSON path.

**Behavior changes (fixes):**

- SQL completes after `--` inside a literal (`'a -- b' FR`), and not inside `/* */` comments.
- SQL suggests literals anywhere inside a closed literal (`'not Fra|'`), not only right after the quote.
- Still the same: a literal that is not closed yet is not a string token (it is not highlighted as one either), so the
  word right after a `'` keeps counting as inside a literal. With bracket closing on, typing `'` closes the literal
  anyway.

### ✅ 1e. Move the "typed word" filter out of `CodeEditor`

**Was:** `CodeEditor` dropped every completion whose `label` equaled the prefix
(`completions.filter(c => c.label !== prefix)`). That existed only because `getPossibleSqlReferences` picked up the
word being typed as a reference.

**Done:** `getPossibleSqlReferences` and `getSqlLiterals` take the prefix and leave it out, and the filter is gone from
the generic component. A spec covers both.

**Behavior change:** a keyword, function or other suggestion that exactly matches what was typed (same case) is now
listed. For example, after typing `FROM` the list still shows `FROM`, so Enter accepts it instead of inserting a
newline. That is what already happened when typing in lowercase (`from` is not equal to `FROM`), and what Ace did
whenever the list had more than that one item.

### ⬜ 1f. Word characters per language

`PREFIX_REGEXP` in `completion-source.ts` is Ace's identifier regex (`$`, `-` and Unicode ranges), the same for every
language. In CodeMirror, word characters are language data (`wordChars`) or the source's own `matchBefore`.

**Decide first:** this changes which part of the text is completed (for example `foo-ba`), so it is a behavior change.

### ✅ 1g. Languages bring their own completion sources

**Was:** the callers chose the completion builder. `FlexibleQueryInput` checked `startsWith('{')` once to pick the mode
and again to pick the completer, and `JsonInput` and `SqlInput` each wrapped their builder by hand in a
`getCompletions` prop, which `CodeEditor` plugged into `autocompletion({ override })`.

**Done:**

- `dsql({ columnMetadata, columns, availableSqlFunctions, skipAggregates })` and `hjson({ jsonCompletions })` attach
  a completion source with `language.data.of({ autocomplete })`. Switching the language switches the completions.
- `makeCompletionSource` (`completion-source.ts`) holds what used to be `CodeEditor`'s `handleCompletions`: it works
  out the word being completed, calls the builder and turns `EditorCompletion`s into `Completion`s.
- `CodeEditor` has no `getCompletions` prop and no `override`, and knows nothing about SQL or Hjson.
- `FlexibleQueryInput` checks `startsWith('{')` once.

**Behavior change (none expected):** a read-only editor now never completes (the source returns nothing), which is
what happened before because read-only editors were never given `getCompletions`.

### ✅ 1h. Leftovers in `hjson-completions.ts`

**Done:** removed the unused `_charBeforePrefix` parameter and the commented-out `quote` line from
`convertToEditorCompletion`.

## 2. Languages

### ✅ 2a. A `language` instead of a `mode` string

**Was:** `mode` (`'dsql' | 'hjson' | 'text'`) is Ace's term, and the string was decoded inside `CodeEditor`
(`modeExtension`), which also added `closeBrackets` for `dsql` only. The Ace term was also in `src/editor-modes/` and
`initDsqlMode`.

**Done:**

- `CodeEditor` takes `language?: LanguageSupport` and puts it in a compartment (no language means plain text).
  `modeExtension` and `CodeEditorMode` are gone.
- `dsql()` includes `closeBrackets()` and its keymap, in the same place in the extension order as before.
- `hjson()` returns the same `LanguageSupport` for the same `jsonCompletions`, so the read-only callers write
  `language={hjson()}`. `dsql()` makes a new one per call, so its callers memoize it.
- `src/editor-modes/` is now `src/editor-languages/` (`editor-languages.spec.ts`).
- The snapshot serializer says `language: dsql` instead of `mode: dsql`. 7 snapshot files changed, only in that
  word.

**Found, not changed:** the `closeBrackets` Backspace binding (delete an empty `()` pair) never runs, because the
default keymap's Backspace comes first. That was so before this step too; it can be fixed separately by moving
`closeBracketsKeymap` ahead of `defaultKeymap`.

### ✅ 2b. No global state for the cluster's SQL functions

**Was:** `initDsqlMode` (called by `ConsoleApplication`) stored the cluster's functions in module-level variables in
`dsql.ts`. Editors that were already open only picked them up on their next edit.

**Done:** `getDsqlLanguage(availableSqlFunctions)` makes one `StreamLanguage` per set of functions (cached in a
`WeakMap`, so the same set gives the same language and does not re-parse). The callers get the functions from
`useAvailableSqlFunctions()` and pass them to `dsql()`. When they arrive, the language compartment is reconfigured and
open editors re-highlight right away. `WorkbenchHistoryDialog` now reads the context too, so its read-only SQL keeps
highlighting the cluster's functions.

### ✅ 2c. Lezer grammars instead of Ace-style rules

**Was:** `createRuleParser` ran the Ace highlight rules (a state stack, push/pop, sticky regexes matched against the
whole line, a guard against zero-width rules that loop) as a `StreamLanguage`. The plan was to keep it and only name it
for what it was, since a grammar would change the tokens.

**Done:** both languages are Lezer grammars, so nothing of the Ace modes is left:

- `dsql.grammar`: the DruidSQL tokens and nested `Parens`. Keywords, functions, constants and types come from an
  external specializer (`dsql-tokens.ts`); `getDsqlLanguage` configures it with the cluster's functions
  (`parser.configure({ specializers })`) instead of building new regex rules.
- `hjson.grammar`: a real Hjson grammar (`Object`, `Property`, `PropertyName`, `Array`, the root object without braces,
  `String` with `Escape`s, `MultilineString`, `QuotelessString`, comments). Quoteless keys and strings depend on the
  context and come from an external tokenizer (`hjson-tokens.ts`).
- `script/build-grammars.mjs` generates the parsers as part of `script/build` (they are not checked in, like
  `lib/sql-docs.ts`). `@lezer/lr` and `@lezer/common` are direct dependencies now, `@lezer/generator` a dev
  dependency.
- The tags are set with `styleTags`. `rule-parser.ts` and `TOKEN_TABLE` are gone, `editorTags` moved to
  `editor-tags.ts`. Hjson keys are `tags.propertyName`, styled like keywords.
- The completions check node names (`LineComment`, `BlockComment`, `Issue`, `String`) instead of the stream tokens.
- Enter still keeps the indentation of the line before (`indentNodeProp` returns `null`), rather than indenting inside
  brackets.

**Checked:** the colors of every SQL and JSON example in the Druid docs, old against new, character by character (see
[MIGRATION.md](./MIGRATION.md#syntax-highlighting)). SQL is the same in 447 of 448. The valid JSON examples are all
highlighted correctly, where the Ace rules got 97 of 452 wrong.

**Visible changes (fixes):** Hjson values that contain a colon (timestamps, `host:port`, URLs) are strings instead of
being split into a key and a number, and quoteless Hjson values are strings as a whole. A word inside an Hjson object
is a key while its colon is still being typed.

## 3. Positions

### ✅ 3a. 1-based `LineColumn` instead of `RowColumn`

**Was:** `RowColumn` was Ace's 0-based `Position`. Druid and Hjson errors report 1-based `line,col`; the console
subtracted 1 (`getRowColumnFromIssue`, `extractRowColumnFromHjsonError`, `DruidError.extractStartRowColumn`) and
`focusEditorAt` added 1 back. The two dialogs that show a parse error position added 1 again for display.

**Done:** `RowColumn` and `offsetToRowColumn` are now `LineColumn` and `offsetToLineColumn` (`src/utils/general.tsx`),
both 1-based, with no conversions left:

- `focusEditorAt(view, { line, column })`
- `DruidError.startLineColumn` / `endLineColumn` (`extractStartLineColumn`, `extractEndLineColumn`)
- `WorkbenchQuery.getLineColumnFromIssue`, `extractLineColumnFromHjsonError`
- `QuerySlice.startLineColumn` / `endLineColumn`; the workbench passes `line - 1` to `changePrefixLines` (the number of
  lines before the slice)
- `SpecDialog` and `ExecutionSubmitDialog` show the position as is

The specs were updated (in `sql.spec.ts` every line and column in the inline snapshots went up by exactly 1).

**Considered, not done:** offsets. The positions come from Druid and Hjson as line and column, and CodeMirror's
`doc.line(n)` takes the same 1-based line, so `LineColumn` is the natural type at the edges.

### ✅ 3b. One way to move the cursor from outside

**Was:** `SqlInput` and `FlexibleQueryInput` each defined the same `goToPosition` imperative handle (a wrapper around
Ace's `moveCursorTo`), and `query-tab.tsx` typed it inline.

**Done:** both pass their `ref` on to the `CodeEditor`, so it gives the `EditorView`, and callers use
`focusEditorAt(view, position)` like `JsonInput` already did. `SqlInputHandle` and `FlexibleQueryInputHandle` are gone.
`FlexibleQueryInput` keeps its own ref too (it reconfigures its sub query compartment), and hands the view on with
`useImperativeHandle`.

## 4. Partial query markers (`FlexibleQueryInput`)

### ✅ 4a. Gutter event handlers instead of class names

**Was:** the row was written into the marker's class (`query-<row>`) and read back from `classList` by React handlers
on a wrapping `div`. That was an Ace limit (breakpoints could only carry a class name).

**Done:** the click and hover come from `lineNumbers({ domEventHandlers: { click, mouseover, mouseout } })`, which
hands over the line. The marker only sets `elementClass = 'sub-query-gutter-marker'`, so it looks the same (same SCSS).
The wrapping `div` has no handlers any more.

The extra `lineNumbers(...)` would show the line numbers on its own, so the markers are only added when the gutter is
shown (`showGutter`), in a `Compartment` that `FlexibleQueryInput` reconfigures, since `extensions` is only read when
the editor is created. Before, they were always added but only visible with the gutter.

### ✅ 4b. Find the queries in the editor state

**Was:** React found the queries (`findAllSqlQueriesInText`) on every change of the query string (plus a 900ms timer
that did the same again), kept them in `lastFoundQueriesRef` and dispatched the rows to the editor. The ref was not
updated by edits while the markers were, which is why the hover highlight had to clamp its range to the document
length.

**Done:** `sub-query-markers.ts` (with a spec) has one `StateField` that finds the queries from the document whenever
it changes, and provides the markers and the hover highlight. The click handler reads the query from that field, so it
always matches the current text. React only provides `onRun` (which shows the "Another query is currently running"
warning or calls `runQuerySlice`). The timer, the refs and the clamp are gone.

**Considered, not done:** debouncing the search. It ran on every change before too (the timer only repeated it), and
doing it in the update keeps the markers in step with the text.

## 5. ✅ Props inherited from react-ace

- ✅ **Read-only was implied by a missing `onChange`** (react-ace's `readOnly={!onChange}` habit). `CodeEditor`,
  `JsonInput` and `FlexibleQueryInput` now take an explicit `readOnly`. The editors that had no `onChange` pass it
  (`ShowJson`, `ShowJsonOrStages`, `ShowValueDialog`, `ExplainDialog`, `WorkbenchHistoryDialog`, the retention
  dialog's default rules and the execution details pane), and `JsonInput` always passes its change handler.
  `SqlInput`'s `onValueChange` is now required (every caller passed one).
- ✅ **Ace names:** `showGutter` is `showLineNumbers` everywhere (`CodeEditor`, `SqlInput`, `FlexibleQueryInput`,
  matching `JsonInput`). `width` / `height` are gone from `CodeEditor`: the fixed sizes moved to the callers' SCSS (on
  the `className` or the wrapping element), and the sizes that change (`JsonInput`'s `height`, `SqlInput`'s `height`)
  go through a new `style` prop.
- ✅ **`padding` was the only display prop that was read once.** It is in a compartment like the others.
- ✅ **The wrappers' names:**
  - `JsonInput`: `focus` → `autoFocus`, `width` removed (no caller passed it), `showLineNumbers` kept
  - `FlexibleQueryInput`: `editorStateId` → `stateCacheId`, `leaveBackground` → `transparentBackground` (defaults to
    `true`, so callers write `transparentBackground={false}`)
  - `SqlInput`: `editorHeight` → `height` (still a number of px). **Kept** `onValueChange`: it is the name that the
    console's other value inputs (`FormattedInput`, `ClearableInput`, `MenuBoolean`, …) use, so renaming it would make
    it the odd one out. `FlexibleQueryInput`'s `onQueryStringChange` is kept for the same reason (it is named after its
    value).
- ✅ **`transparentBackground` depended on the wrapper's class** (`.no-background > &` in the theme). It is now a theme
  (`codeEditorTransparentTheme`) in a compartment, so it no longer depends on the wrapper.

**Checked:** DOM snapshots changed only by the inline `width` / `height` styles and the `no-background` class going
away (19 snapshots). In a browser, the last commit and this change were run side by side: the workbench editor
(size, transparent background), the SQL error link (line 2, column 5), the Hjson "Go to issue" toast (line 4,
column 1), and the `JsonInput` in the coordinator dynamic config dialog (size, background, error link to line 3,
column 7) all came out the same.

## 6. ✅ Low priority

- ✅ **`EditorStateCache`** was the Ace `UndoManager` cache carried over: a public static `Map<string, unknown>` in
  `src/singletons/` that `CodeEditor` wrote to and the workbench cleaned up by hand. **Done:** the saved states are now
  a module-private `Map<string, SavedEditorState>` in `code-editor.tsx` (`SavedEditorState` is what
  `EditorState.toJSON` gives), and `CodeEditor` exports `forgetEditorState(stateCacheId)`, which the workbench calls
  when a tab is closed. `src/singletons/editor-state-cache.ts` is gone. `code-editor.spec.tsx` checks that the undo
  history survives a remount and is gone after `forgetEditorState`.
- ✅ **The shared tooltip host is prepended to `<body>`.** **Done:** kept as is, with the reasons now next to the code
  (the comment on `getTooltipHost`) and in the README's styling notes:
  - In the body so that the editor wrapper (`overflow: hidden`), dialogs and popovers don't clip the tooltips, like
    Ace's popup. CodeMirror gives tooltips `z-index: 500`, above Blueprint's overlays (20) and toasts (40).
  - At the start of the body, not the end, because Blueprint appends its portals to the body and the dialog specs
    snapshot `document.body.lastChild`.
  - One host for all editors, created the first time one mounts.

## Leave as is

These are deliberate choices that keep the behavior and the look, not Ace leftovers:

- `diffStrings` and the controlled `value`
- `insertSoftTab`
- `positionInfo`
- the theme and its pre-brightened colors
