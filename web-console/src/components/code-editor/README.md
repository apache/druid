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

# CodeEditor

`CodeEditor` is the web console's code editor: a React wrapper around [CodeMirror 6](https://codemirror.net/). Use it
whenever the console shows or edits SQL or JSON: the workbench query input, the explore view's SQL inputs, JSON spec
dialogs, read-only JSON views and so on.

It is a controlled component that you give a string and an `onChange` callback. It comes with the console's look
(syntax colors, gutter, autocomplete popup), DruidSQL and Hjson highlighting and context-aware autocomplete.

## Files

| File                    | What it holds                                                                    |
| ----------------------- | -------------------------------------------------------------------------------- |
| `code-editor.tsx`       | The component, its props, the CodeMirror setup and the `focusEditorAt` helper    |
| `code-editor-theme.ts`  | The editor theme (layout, gutter, popups) and the syntax highlighting colors     |
| `completion-doc.ts`     | Renders the documentation panel shown next to the completion list               |
| `doc-markdown.ts`       | Renders "doc markdown" (the format of the SQL docs) as DOM elements             |
| `code-editor.scss`      | The few styles for the wrapper element that can't live in the CodeMirror theme  |

Related code lives elsewhere:

| Location                               | What it holds                                                           |
| -------------------------------------- | ----------------------------------------------------------------------- |
| `src/editor-modes/`                    | The `dsql` and `hjson` languages (syntax highlighting rules)            |
| `src/editor-completions/`              | What to suggest when autocompleting SQL and Hjson                       |
| `src/singletons/editor-state-cache.ts` | Keeps editor state (undo history, selection) between mounts             |

## Basic usage

```tsx
import { CodeEditor } from '../../components';

// Editable SQL
<CodeEditor mode="dsql" value={sql} onChange={setSql} height="300px" showGutter />

// Read-only JSON (leaving out onChange makes the editor read-only)
<CodeEditor mode="hjson" value={JSON.stringify(spec, undefined, 2)} height="100%" />
```

## Props

| Prop                    | Type                         | Notes                                                                                                         |
| ----------------------- | ---------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `value`                 | `string`                     | Required. The text to show.                                                                                   |
| `onChange`              | `(value: string) => void`    | Called when the **user** edits the text. **If omitted, the editor is read-only.**                             |
| `onBlur`                | `() => void`                 |                                                                                                               |
| `mode`                  | `'dsql' \| 'hjson' \| 'text'` | The language, which controls highlighting, comment toggling and bracket auto-closing. Defaults to `'text'`.   |
| `width` / `height`      | `string`                     | CSS sizes for the wrapper. With no `height`, the editor grows with its content.                               |
| `className`             | `string`                     | Added to the wrapper `div` (which always has the `code-editor` class).                                        |
| `showGutter`            | `boolean`                    | Shows line numbers.                                                                                           |
| `padding`               | `number`                     | Pads the text on all sides (in px). By default there is only a little horizontal padding.                     |
| `transparentBackground` | `boolean`                    | Drops the editor's own background.                                                                            |
| `placeholder`           | `string`                     | Shown while the editor is empty.                                                                              |
| `autoFocus`             | `boolean`                    | Focuses the editor when it mounts.                                                                            |
| `getCompletions`        | `(request) => EditorCompletion[]` | Turns on autocomplete. See [Autocomplete](#autocomplete).                                                |
| `stateCacheId`          | `string`                     | Remembers the undo history and selection under this id, so they survive the editor being unmounted.           |
| `extensions`            | `Extension`                  | Extra CodeMirror extensions. **Only read when the editor is created.**                                        |
| `ref`                   | `Ref<EditorView>`            | Gives you the underlying CodeMirror `EditorView`.                                                             |

## How it works

### The value is controlled

The editor keeps its own CodeMirror document, and the component keeps that document in sync with `value`:

- When the user types, `onChange` is called with the full new text.
- When `value` changes from the outside (formatting a query, inserting a column name), the editor applies **only the
  part that changed**, so the cursor stays put and the change can be undone. External changes do **not** call
  `onChange`.

You can therefore treat it like an `<input>`. If the parent rewrites or rejects what was typed, the editor shows what
the parent passes back.

### Props that change vs. props read once

Most props (`mode`, `onChange`/read-only, `showGutter`, `placeholder`, `getCompletions`, `onBlur`) can change at any
time. The editor updates in place without being recreated. The workbench input uses this to switch between `dsql`
and `hjson` as soon as the text starts with `{`.

`padding`, `extensions` and `autoFocus` are only read when the editor is created. Changing `stateCacheId` recreates
the editor.

### Remembering state between mounts

When `stateCacheId` is set, the editor saves its state (undo history and selection) when it unmounts. When an editor
with the same id mounts again, it restores that state. The workbench uses this so that undo still works after you
switch tabs and come back. If `value` changed while the editor was away, the restored editor is updated to match.

Remove the saved state with `EditorStateCache.deleteState(id)` once it is no longer needed (the workbench does this
when a tab is closed).

## Autocomplete

Pass `getCompletions` to turn on autocomplete. Suggestions appear as the user types a word, and Ctrl-Space opens the
list on demand. Enter or Tab accepts the selected one. The callback gets a `CompletionRequest` describing where the
cursor is:

```ts
interface CompletionRequest {
  allText: string; // The whole text
  prefix: string; // The (partial) word being completed
  charBeforePrefix: string; // The character before it: '\n' at the start of a line, '' at the start of the text
  textBeforePrefix: string; // Everything before charBeforePrefix
  lineBeforePrefix: string; // The part of the current line before charBeforePrefix
}
```

It returns `EditorCompletion`s (from `src/editor-completions/editor-completion.ts`):

```ts
interface EditorCompletion {
  label: string; // What gets inserted (and matched against what was typed)
  displayLabel?: string; // What is shown in the list (defaults to label)
  detail?: string; // Shown on the right, like 'column' or 'function'
  boost?: number; // Ranks equally good matches, from -99 to 99, higher first
  doc?: CompletionDoc; // Shown in a panel next to the list when the item is selected
}

interface CompletionDoc {
  name: string; // The title
  syntax?: string; // Plain text in monospace under the title, like a function signature
  description?: string; // Plain text
  descriptionMarkdown?: string; // "Doc markdown", the simplified markdown of the docs in lib/sql-docs.ts
}
```

The field names are the ones of CodeMirror's `Completion`. The editor renders the doc panel itself
(`completion-doc.ts`), so completion builders only describe what to show. No HTML strings are involved: plain text is
inserted as text, and "doc markdown" is turned into elements by `renderDocMarkdown` (`doc-markdown.ts`).

"Doc markdown" is what `script/create-sql-docs.mjs` produces from the Druid docs for `lib/sql-docs.ts`. It only has
`` `code` ``, `*emphasis*`, line breaks (`\n`), list items (lines starting with `- `) and backslash escapes. The script
drops links, converts `<br>` and simple lists, and fails on any other HTML.

Usually you don't write completions yourself. Hook up the existing builders:

```tsx
const getCompletions = useCallback(
  ({ allText, prefix, charBeforePrefix, lineBeforePrefix }: CompletionRequest) =>
    getSqlCompletions({ allText, prefix, charBeforePrefix, lineBeforePrefix, columnMetadata, availableSqlFunctions }),
  [columnMetadata, availableSqlFunctions],
);

<CodeEditor mode="dsql" value={sql} onChange={setSql} getCompletions={getCompletions} />;
```

Things to know:

- The callback can change between renders, and the latest one is always used.
- The editor filters and ranks your list against the prefix (fuzzy matching), so you don't need to filter it yourself.
- The popup and its doc panel are rendered in a shared container at the start of `<body>`, so they are never clipped by
  dialogs or popovers that contain the editor.

## Working with the `EditorView`

For anything beyond the props, use `ref` to get the CodeMirror `EditorView`:

```tsx
const viewRef = useRef<EditorView | undefined>(undefined);

<CodeEditor ref={viewRef} value={sql} onChange={setSql} />;

// Later: put the cursor on row 3, column 5 (both 0-based) and focus the editor
if (viewRef.current) focusEditorAt(viewRef.current, { row: 3, column: 5 });
```

`focusEditorAt` is exported next to the component. It clamps out-of-range positions and scrolls the cursor into view.
It is used to jump to the location of query errors.

If you need decorations, gutter markers or other custom behavior, pass CodeMirror extensions through `extensions`.
`FlexibleQueryInput` (`src/views/workbench-view/flexible-query-input/`) is the example to read. It adds:

- a `StateField` that puts a "run this query" marker on the line numbers (via the `lineNumberMarkers` facet), with the
  `sub-query-gutter-marker query-<row>` classes on the line-number cell
- a `StateField` with a mark decoration that highlights a query while its marker is hovered

Both are changed with `StateEffect`s dispatched on the view. Clicks and hovers are handled by plain React handlers on
a wrapping `div`.

## Languages

The modes are CodeMirror `StreamLanguage`s defined in `src/editor-modes/`:

- **`dsql`** (DruidSQL): Keywords, functions, data types and constants come from `lib/keywords.ts` and
  `lib/sql-docs`. Double-quoted references (`"column"`) and `--:ISSUE:` comments get their own colors.
  `initDsqlMode(availableSqlFunctions)` adds the functions that the cluster reports, and is called once the
  capabilities are known. The SQL mode auto-closes `(`, `[`, `'` and `"` (not `{`), and Cmd/Ctrl-/ toggles `--`
  comments.
- **`hjson`** (Hjson/JSON): highlights keys, strings, numbers, escapes and `#`, `//` and `/* */` comments, including
  objects without the outer braces.

Both are built with `createRuleParser` (`src/editor-modes/rule-parser.ts`). It takes Ace-style rules: for each state, a
list of regexes tried in order, where a rule can push or pop a state. Token names map to highlighting tags through
`TOKEN_TABLE` in the same file.

To add a token type, add it to `TOKEN_TABLE`, emit it from a rule, and give its tag a color in
`codeEditorHighlightStyle` (in `code-editor-theme.ts`).

## Styling

The theme reproduces the look of the Ace editor the console used before: Ace's `solarized_dark` theme plus the
console's overrides. Most of it is in `code-editor-theme.ts` as an `EditorView.theme(...)`, which CodeMirror scopes to
the editor and to its tooltips. Some things to know before changing it:

- **The token colors are pre-brightened.** The Ace theme drew text through `filter: brightness(1.5) saturate(0.9)`.
  The colors in `codeEditorHighlightStyle` are the result of that filter. Don't add a CSS filter to `.cm-content`,
  because it would also brighten the active-line and highlight backgrounds.
- **Console colors are copied as constants.** The theme lives in TypeScript, so the few SCSS colors it needs
  (`$dark-gray1`, `$dark-gray4`, `$gray1`, `$gray5`) are copied in with a comment pointing to
  `blueprint-overrides/common/_colors.scss`. Keep them in sync if those change.
- **Mind selector specificity.** CodeMirror's base theme uses selectors like `&dark .cm-gutters`. A theme rule wins
  when its selector is as specific as the base one. `codeEditorPaddingTheme` uses slightly more specific selectors on
  purpose so it beats the main theme.
- **The wrapper** gets `position: relative; overflow: hidden` (in `code-editor.scss`), like Ace's root element, so the
  editor never pushes past the size it is given in flex layouts.
- **Style it from the outside** with the wrapper's `className` (for size, borders and layout), or `.cm-*` class names
  under it when you need to.

## Keyboard

The standard CodeMirror keymaps are on: editing, undo/redo, search (Cmd/Ctrl-F) and autocomplete. Tab behaves like it
did in Ace. With nothing selected it inserts spaces up to the next 2-column tab stop. With a selection it indents the
selected lines, and Shift-Tab un-indents.

## Testing

DOM snapshots don't include CodeMirror's internal DOM. The snapshot serializer (`src/test-utils/snapshot-serializer.ts`)
replaces each editor with a comment listing what the console configured, for example:

```html
<div class="code-editor query-string" style="height: 100%;">
  <div class="cm-editor">
    <!-- Code editor, mode: hjson, value: "{\n  \"a\": 1\n}", read only -->
  </div>
</div>
```

So a snapshot changes only when the mode, value, placeholder or read-only state changes, not when CodeMirror is
upgraded.

The language rules are tested directly in `src/editor-modes/editor-modes.spec.ts`. It parses text with the language's
parser and checks the token name produced for each piece of text.
