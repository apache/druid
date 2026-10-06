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

# Migration from Ace to CodeMirror 6

This document records how the web console moved from the [Ace](https://ace.c9.io/) editor (through `react-ace`) to
[CodeMirror 6](https://codemirror.net/). The goal was a drop-in replacement: same features and as close to the same
look as possible. For how to use the new component, see [README.md](./README.md).

## Summary

- `ace-builds` and `react-ace` were removed. `@codemirror/{state,view,language,autocomplete,commands,search}` and
  `@lezer/highlight` were added.
- All 10 places that rendered `<AceEditor>` now render the new `CodeEditor` component (`src/components/code-editor/`).
- The custom DruidSQL (`dsql`) and Hjson (`hjson`) Ace modes were ported to CodeMirror `StreamLanguage`s.
- The SQL and Hjson completion builders were kept. Their return type changed from Ace's `ValueCompletion` to a
  console-owned `EditorCompletion` that uses the field names of CodeMirror's `Completion`.
- The workbench's "run this query" gutter markers and hover highlight were reimplemented as CodeMirror extensions.
- The styling reproduces Ace's `solarized_dark` theme with the console's overrides. It was compared side by side
  against master in the browser.

## What moved where

| Before (Ace)                                   | After (CodeMirror)                                                       |
| ---------------------------------------------- | ------------------------------------------------------------------------ |
| `react-ace`'s `<AceEditor>`                    | `src/components/code-editor/code-editor.tsx` (`CodeEditor`)              |
| `src/bootstrap/ace.ts` (imports, theme, modes) | Removed. Everything is imported where it is used                         |
| `src/bootstrap/ace.scss` (theme overrides)     | `src/components/code-editor/code-editor-theme.ts` (+ `code-editor.scss`) |
| `src/ace-modes/dsql.ts`, `hjson.ts`            | `src/editor-languages/dsql.ts`, `hjson.ts`                               |
| `src/ace-modes/ace-mode-helpers.ts`            | `src/editor-languages/rule-parser.ts` (Ace-style rules → stream parser)  |
| `src/ace-modes/ace-modes.spec.ts`              | `src/editor-languages/editor-languages.spec.ts`                          |
| `initAceDsqlMode(functions)`                   | `dsql({ availableSqlFunctions })` (no global state)                      |
| `mode="dsql"` / `mode="hjson"`                 | `language={dsql(...)}` / `language={hjson(...)}` (`LanguageSupport`s)    |
| `setCompleters` / `getCompletions` prop        | The language's completion source (`language.data.of({ autocomplete })`)  |
| `src/ace-completions/*`                        | `src/editor-completions/*` (+ `editor-completion.ts` for the type)       |
| `makeDocHtml` (HTML strings in `docHTML`)      | Structured `doc` rendered by `completion-doc.ts`                         |
| `Ace.ValueCompletion`                          | `EditorCompletion` (`label`, `displayLabel`, `boost`, `detail`, `doc`)   |
| `src/singletons/ace-editor-state-cache.ts`     | `src/singletons/editor-state-cache.ts` (`EditorStateCache`)              |
| `editor.getSelection().moveCursorTo(row, col)` | `focusEditorAt(view, { row, column })`                                   |

### Call sites

| Component                        | Notes                                                                                            |
| -------------------------------- | ------------------------------------------------------------------------------------------------ |
| `FlexibleQueryInput` (workbench) | Rewritten. See [Partial query markers](#partial-query-markers)                                   |
| `SqlInput` (explore view)        | Same props and behavior                                                                          |
| `JsonInput`                      | Same props. The change handler is only passed when `onChange` is given (that makes it read-only) |
| `ShowJson`, `ShowJsonOrStages`   | Read-only Hjson                                                                                  |
| `SpecDialog`                     | `height="500px"` is now explicit (it used to come from react-ace's default)                      |
| `ShowValueDialog`                | Its SCSS targeted `.ace-editor`, a class Ace never set. It now targets `.code-editor`            |
| `WorkbenchHistoryDialog`         | Read-only, `dsql` or `hjson`                                                                     |
| `ExplainDialog`                  | Read-only Hjson                                                                                  |

## Behavior parity

These Ace behaviors were deliberately preserved:

- **Controlled value:** react-ace replaced the text on prop changes without calling `onChange`. `CodeEditor` does the
  same, but it applies only the changed part, so the cursor stays where it was and the change is undoable.
- **Read-only:** `readOnly={!onChange}` was the pattern everywhere, so the editor is now read-only exactly when
  `onChange` is not given.
- **Autocomplete triggering:** like Ace's live autocompletion, the list opens while typing a word (Ace's identifier
  characters, including `$` and `-`) and on Ctrl-Space. Tab accepts a suggestion as well as Enter.
- **Autocomplete inputs:** the old completers worked out `charBeforePrefix`, `lineBeforePrefix` and `textBefore`
  from the Ace session. The builders now get CodeMirror's `CompletionContext` and the word being completed, and read
  the syntax tree to tell comments and strings apart. The callers no longer pick the completer: `dsql(...)` and
  `hjson(...)` bring theirs.
- **Ranking:** Ace's `score` became CodeMirror's `boost` (the scores were already within its ±99 range). CodeMirror ranks by
  match quality first and boost second, which gave the same ordering in practice.
- **Doc tooltip:** the `doc` is shown in a 500px panel next to the list, styled like the old `.ace_tooltip`
  (`doc-name`, `doc-syntax` classes).
- **Tab:** with nothing selected, Tab inserts spaces to the next 2-column stop; with a selection it indents the lines.
  This matches Ace (CodeMirror does not bind Tab by default).
- **Bracket behavior:** Ace auto-closed brackets in `dsql` (`CstyleBehaviour`) but not in `hjson`. The same applies
  now, except that `{` is not auto-closed in SQL. See [Intentional differences](#intentional-differences).
- **Comment toggling:** Cmd/Ctrl-/ uses `--` for `dsql`, and `//` and `/* */` for `hjson`.
- **Undo across tabs:** the old cache kept Ace's `UndoManager` per workbench tab. The new cache keeps
  `EditorState.toJSON({ history })` and restores it with `EditorState.fromJSON`, so undo works after switching tabs.
- **Highlighting the cluster's functions:** `dsql({ availableSqlFunctions })` adds them to the highlighting. With Ace
  (a global `initAceDsqlMode`), only editors created afterwards picked them up. Now the open editors are reconfigured
  with the new language and re-highlight right away.

### Syntax highlighting port

The Ace modes were defined as Ace highlight rules: per state, a list of regexes tried in order, with push/pop of
states. `createRuleParser` runs rules of the same shape as a CodeMirror stream parser, so the rules were ported nearly
verbatim. A few details that keep the results identical:

- Rules are compiled as **sticky regexes matched against the whole line**, not a slice of it, so `\b` and `$` behave
  as they did in Ace.
- Zero-width rules (Hjson's "object without braces" lookahead) can push a state without consuming input, as in Ace.
- When several keyword lists contain the same word, **the last list wins** (for example, a data type that is also a
  keyword is highlighted as a type). This matches Ace's `createKeywordMapper`.
- Token names map to Lezer tags through `TOKEN_TABLE`. `--:ISSUE:` comments and double-quoted column references got
  custom tags (`editorTags.issue`, `editorTags.column`), as they had custom Ace token classes.

### Partial query markers

In `FlexibleQueryInput`, a play button in the gutter runs a single query when the text contains several.

- **Before:** markers were Ace "breakpoints" (`session.setBreakpoint(row, className)`). The hover highlight was an Ace
  text marker (`session.addMarker(new Range(...))`).
- **After:**
  - `subQueryMarkers(onRun)` (`sub-query-markers.ts`) is a CodeMirror extension. Its `StateField` finds the queries
    in the document on every change and provides the markers to the `lineNumberMarkers` facet. Each marker sets
    `elementClass = 'sub-query-gutter-marker'` on its line-number cell. The existing SCSS (the blue square and
    triangle drawn with `:before`/`:after`) works unchanged apart from adding `position: relative`.
  - The same field provides a `Decoration.mark` with the `sub-query-highlight` class over the hovered query.
  - Clicks and hovers come from `lineNumbers({ domEventHandlers })`, which hands over the line. Ace's breakpoints
    could only carry a class name, so the row used to be written into a `query-<row>` class and read back by React
    handlers on a wrapping `div`.
  - The `ResizeSensor` that fed Ace an explicit pixel height is gone, because the editor now fills its container with
    CSS.

## Styling

The look was matched by comparing screenshots of the workbench on master and on this branch, both taken with
Playwright at 2× scale.

- **Token colors:** the old theme drew text through `filter: brightness(1.5) saturate(0.9)`. In CodeMirror, the
  active-line and highlight backgrounds live inside the content element, so the same filter would brighten them too.
  The filter was applied to each solarized color ahead of time instead:

  | Token                        | Ace class                  | Solarized | Rendered (used now) |
  | ---------------------------- | -------------------------- | --------- | ------------------- |
  | Default text                 |                            | `#839496` | `#c7dde0`           |
  | Keyword                      | `keyword`                  | `#859900` | `#c8e315`           |
  | Function                     | `support.function`         | `#268bd2` | `#45cef7`           |
  | Constant / escape            | `constant.language`        | `#b58900` | `#facd14`           |
  | Data type                    | `storage.type` (custom)    | `#27c923` | `#49f943`           |
  | Number                       | `constant.numeric`         | `#d33682` | `#f256bc`           |
  | String                       | `string`                   | `#2aa198` | `#4deee1`           |
  | Comment (italic)             | `comment`                  | `#657b83` | `#9ab8c3`           |
  | `--:ISSUE:` (wavy underline) | `comment.issue` (custom)   | `#cb3116` | `#f04d29`           |
  | Column reference             | `variable.column` (custom) | `#2ceefb` | `#51fbfb`           |

- **Metrics:** 12px Monaco/Menlo stack with `line-height: normal` (≈16px lines, like Ace). The gutter cells are padded
  `0 13px 0 21px` to match Ace's gutter width. `padding={10}` reproduces `renderer.setPadding(10)` plus
  `setScrollMargin(10, 10)`.
- **Chrome:** background `rgba($dark-gray1, 0.5)` (or transparent), gutter `$dark-gray4` / `$gray5`, active line
  `rgba(255, 255, 255, 0.1)`, active gutter cell `rgba($gray1, 0.6)`, 2px `#d30102` cursor, outlined matching bracket.
- **Autocomplete popup:** `$dark-gray4` background with no border, a 2px radius and the console's shadow. It is 300px
  wide with 1.4 line height and up to 8 rows. The selected row is `#3a674e`, matched letters `#a2de14`, and the meta
  text is right-aligned at 50% opacity.
- **Placeholder:** Arial, scaled to 0.9, italic, `#657b83` at 70% opacity. The old `placeholder-padding` hack is gone,
  because the CodeMirror placeholder sits inside the padded content.

## Intentional differences

- **The typed word is not suggested.** The SQL completer suggests words found in the text, which includes the
  half-typed word itself. Ace gathered suggestions after the first character (too short to count as a reference) and
  then only filtered them, so the typed word never appeared. CodeMirror can gather them later, so the SQL completer
  now leaves the typed word out of the references and literals it finds.
- **`{` is not auto-closed in SQL.** Ace only "maybe" inserted the closing brace (it added it on Enter). Typing `{` into
  the workbench usually starts a native JSON query, and an eager `}` was left behind once the mode switched to Hjson.
- **No HTML in docs.** Ace's `docHTML` took every doc as HTML, so Hjson docs for values like `ARRAY<STRING>` showed
  up as `ARRAY`. A `doc` is now plain text, or "doc markdown" for the SQL docs, and is rendered as elements.
  `script/create-sql-docs.mjs` produces doc markdown instead of HTML (snarkdown was removed), which also fixed a few
  SQL docs: a single `<br>` in the docs is now a line break (snarkdown turned it into a space or dropped it), and a
  literal `*` in the ATAN2 doc no longer starts italics.
- **Functions with several signatures are listed once.** Ace listed one item per signature with the signature as the
  caption (`BIG_MAX(<ANY>)`, `BIG_MAX(column, size)`, …). CodeMirror drops options with the same label and detail, so
  only the first signature was left. The list now shows the name (`BIG_MAX`), and the doc panel shows every signature,
  one per line.
- **Undo granularity:** CodeMirror groups typing into undo steps differently from Ace.
- **Tooltips:** all editors share one tooltip container that is _prepended_ to `<body>`. Like Ace's popup, it can't be
  clipped by the editor's containers. Prepending also keeps `document.body.lastChild` pointing at Blueprint portals,
  which about 25 dialog specs snapshot.

## Known gaps

- **Placeholder offset:** the placeholder text starts about 6px further left than Ace's did.
- **Long captions:** they are cut off with "…" at the end. Ace shortened them so that the matched part stayed visible.
- **Not checked visually:** the explore-view filter and measure popovers (`SqlInput` inside Blueprint popovers), the
  spec dialog, the history dialog and the explain dialog. They use the same component. The popovers deserve a look,
  since the autocomplete renders in a body-level container there.

## Tests

- `editor-languages.spec.ts` (was `ace-modes.spec.ts`) was rewritten to tokenize with the CodeMirror parser. It covers
  the same cases as before, plus block comments, arrays and brace-less Hjson objects, and that the languages bring
  their completions.
- The snapshot serializer replaces each `.cm-editor` with a comment (language, value, placeholder, read-only) and drops
  CodeMirror's generated theme classes. 10 snapshots were updated; in every case only the wrapper markup changed.
- `explain-dialog.spec.tsx` mocked the entire `hooks` module, which removed hooks that `CodeEditor` uses. The mock now
  spreads `jest.requireActual` and overrides only `useQueryManager`.
- Checked in a running console: the gutter markers run the clicked query, undo history survives switching tabs, and
  both modes highlight like master.

## Licensing

`licenses.yaml` was regenerated with `script/licenses`. The CodeMirror packages and their dependencies (`@lezer/*`,
`style-mod`, `w3c-keyname`, `crelt`, `@marijn/find-cluster-break`, all MIT) were added under `licenses/bin`. The
entries and license files for `ace-builds`, `react-ace` and `fast-equals` (a react-ace dependency) were removed;
`diff-match-patch`, another react-ace dependency, dropped out of `licenses.yaml` too.
