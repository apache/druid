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

| File                   | What it holds                                                                 |
| ---------------------- | ----------------------------------------------------------------------------- |
| `code-editor.tsx`      | The component, its props, the CodeMirror setup and the `focusEditorAt` helper |
| `code-editor-theme.ts` | The editor theme (layout, gutter, popups) and the syntax highlighting colors  |
| `completion-source.ts` | Turns a completion builder into a CodeMirror completion source                |
| `completion-doc.ts`    | Renders the documentation panel shown next to the completion list             |
| `doc-markdown.ts`      | Renders "doc markdown" (the format of the SQL docs) as DOM elements           |
| `search-panel.tsx`     | The find (and replace) panel, made out of Blueprint components                |
| `code-editor.scss`     | The styles for the wrapper element and the find panel                         |

Related code lives elsewhere:

| Location                  | What it holds                                                       |
| ------------------------- | ------------------------------------------------------------------- |
| `src/editor-languages/`   | The `dsql()` and `hjson()` languages (highlighting and completions) |
| `src/editor-completions/` | What to suggest when autocompleting SQL and Hjson                   |

## Basic usage

```tsx
import { CodeEditor } from '../../components';
import { dsql } from '../../editor-languages/dsql';
import { hjson } from '../../editor-languages/hjson';

// Editable SQL (memoize the language, see Languages)
const sqlLanguage = useMemo(() => dsql({ availableSqlFunctions }), [availableSqlFunctions]);
<CodeEditor language={sqlLanguage} value={sql} onChange={setSql} showLineNumbers />

// Read-only JSON, sized with CSS on its className
<CodeEditor className="spec-viewer" language={hjson()} value={JSON.stringify(spec, undefined, 2)} readOnly />
```

## Props

| Prop                    | Type                      | Notes                                                                                                                        |
| ----------------------- | ------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `value`                 | `string`                  | Required. The text to show.                                                                                                  |
| `onChange`              | `(value: string) => void` | Called when the **user** edits the text.                                                                                     |
| `readOnly`              | `boolean`                 | Makes the editor read-only.                                                                                                  |
| `onBlur`                | `() => void`              |                                                                                                                              |
| `language`              | `LanguageSupport`         | `dsql(...)` or `hjson(...)`. Brings highlighting, completions, comment toggling and bracket closing. Plain text without one. |
| `className`             | `string`                  | Added to the wrapper `div` (which always has the `code-editor` class). Size the editor with CSS on it.                       |
| `style`                 | `CSSProperties`           | Inline styles for the wrapper, for a size that changes (like `JsonInput`'s `height`). With no height it grows with the text. |
| `showLineNumbers`       | `boolean`                 | Shows the line numbers.                                                                                                      |
| `padding`               | `number`                  | Pads the text on all sides (in px). By default there is only a little horizontal padding.                                    |
| `transparentBackground` | `boolean`                 | Drops the editor's own background.                                                                                           |
| `placeholder`           | `string`                  | Shown while the editor is empty.                                                                                             |
| `autoFocus`             | `boolean`                 | Focuses the editor when it mounts.                                                                                           |
| `stateCacheId`          | `string`                  | Remembers the undo history and selection under this id, so they survive the editor being unmounted.                          |
| `extensions`            | `Extension`               | Extra CodeMirror extensions. **Only read when the editor is created.**                                                       |
| `ref`                   | `Ref<EditorView>`         | Gives you the underlying CodeMirror `EditorView`.                                                                            |

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

Most props (`language`, `onChange`, `readOnly`, `showLineNumbers`, `padding`, `transparentBackground`, `placeholder`,
`onBlur`) can change at any time. The
editor updates in place without being recreated. The workbench input uses this to switch between `dsql()` and
`hjson()` as soon as the text starts with `{`. The editor is reconfigured whenever `language` is a new object, so keep
it stable between renders (see [Languages](#languages)).

`extensions` and `autoFocus` are only read when the editor is created. Changing `stateCacheId` recreates
the editor.

### Remembering state between mounts

When `stateCacheId` is set, the editor saves its state (undo history and selection) when it unmounts. When an editor
with the same id mounts again, it restores that state. The workbench uses this so that undo still works after you
switch tabs and come back. If `value` changed while the editor was away, the restored editor is updated to match.

The saved states are kept inside `code-editor.tsx`. Call `forgetEditorState(id)` once an id will not be shown again
(the workbench does this when a tab is closed).

## Autocomplete

The completions come with the language: `dsql(...)` always completes and `hjson({ jsonCompletions })` completes when
given rules. Each attaches a CodeMirror completion source as language data (`language.data.of({ autocomplete })`), so
switching the language switches the completions. Suggestions appear as the user types a word, and Ctrl-Space opens the
list on demand. Enter or Tab accepts the selected one. Read-only editors don't complete.

The completion builders in `src/editor-completions/` (`getSqlCompletions`, `getHjsonCompletions`) are
`EditorCompletionBuilder`s. `makeCompletionSource` (`completion-source.ts`) turns one into a completion source: it
works out the word being completed, skips read-only editors, and calls the builder with CodeMirror's
`CompletionContext` and the word:

```ts
type EditorCompletionBuilder = (
  context: CompletionContext, // CodeMirror's: state, pos, explicit, matchBefore(...)
  word: CompletionWord, // { from, text }: the (partial) word being completed, it ends at pos
) => readonly EditorCompletion[];
```

Builders read whatever text they need from `context.state`. To find out what the cursor is in, they look at the syntax
tree that the language builds (`tokenBefore(state, pos)` in `completion-source.ts` gives the token name, like
`comment` or `string`) instead of looking for `--` or quotes in the text. In specs, `completionContextAt(dsql(),
'SELECT * FROM t WHERE |')` (`src/test-utils/completion-context.ts`) makes the context for the cursor at the `|`.

A builder returns `EditorCompletion`s (from `src/editor-completions/editor-completion.ts`):

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

Usually you don't write completions yourself. Pass what the builders need to the language:

```tsx
const language = useMemo(
  () => dsql({ columnMetadata, availableSqlFunctions }),
  [columnMetadata, availableSqlFunctions],
);

<CodeEditor language={language} value={sql} onChange={setSql} />;
```

Things to know:

- The editor filters and ranks your list against the prefix (fuzzy matching), so you don't need to filter it yourself.
- The popup and its doc panel are rendered in a shared container at the start of `<body>`, so they are never clipped by
  dialogs or popovers that contain the editor.

## Working with the `EditorView`

For anything beyond the props, use `ref` to get the CodeMirror `EditorView`:

```tsx
const viewRef = useRef<EditorView | undefined>(undefined);

<CodeEditor ref={viewRef} value={sql} onChange={setSql} />;

// Later: put the cursor on line 3, column 5 (both 1-based) and focus the editor
if (viewRef.current) focusEditorAt(viewRef.current, { line: 3, column: 5 });
```

`focusEditorAt` is exported next to the component. It takes a `LineColumn` (from `src/utils/general.tsx`): 1-based,
the way Druid and Hjson errors report positions and the way CodeMirror numbers lines, so error positions are passed
straight through. It clamps out-of-range positions and scrolls the cursor into view. It is used to jump to the location
of query errors.

`SqlInput` and `FlexibleQueryInput` pass their `ref` on to the `CodeEditor`, so their callers get the `EditorView` the
same way. For example, the workbench calls `focusEditorAt(queryInputRef.current, position)` to jump to an error.

If you need decorations, gutter markers or other custom behavior, pass CodeMirror extensions through `extensions`.
The "run this query" markers of `FlexibleQueryInput` (`sub-query-markers.ts` in
`src/views/workbench-view/flexible-query-input/`) are the example to read. `subQueryMarkers(onRun)` is one extension
made of:

- a `StateField` that finds the queries in the document whenever it changes, and provides the markers to the
  `lineNumberMarkers` facet and the hover highlight to `EditorView.decorations`
- `lineNumbers({ domEventHandlers })` for the click and hover, which hands over the line that was clicked or hovered
- a `StateEffect` to set the hovered query

React only provides `onRun`. Because `extensions` is only read once, `FlexibleQueryInput` puts the markers in its own
`Compartment` and reconfigures it when they are turned on or off.

## Languages

`src/editor-languages/` exports a `LanguageSupport` factory per language. A `LanguageSupport` is CodeMirror's bundle
of a language and the extensions that go with it:

- **`dsql({ availableSqlFunctions, columnMetadata, columns, skipAggregates })`** (DruidSQL): Keywords, functions,
  data types and constants come from `lib/keywords.ts` and `lib/sql-docs`, plus the `availableSqlFunctions` that the
  cluster reports (from `useAvailableSqlFunctions()`). Double-quoted references (`"column"`) and `--:ISSUE:` comments
  get their own colors. It auto-closes `(`, `[`, `'` and `"` (not `{`), and Cmd/Ctrl-/ toggles `--` comments. The
  options are passed on to `getSqlCompletions`. Each call makes a new `LanguageSupport`, so memoize it.
- **`hjson({ jsonCompletions })`** (Hjson/JSON): highlights keys, strings, numbers, escapes and `#`, `//` and `/* */`
  comments, including objects without the outer braces. It returns the same object for the same `jsonCompletions`,
  so `hjson()` can be called while rendering.

The highlighting of `dsql` depends on the cluster's functions, so `getDsqlLanguage(availableSqlFunctions)` makes one
`StreamLanguage` per set of functions (and the same one for the same set). When the functions arrive, the editors that
are open re-highlight right away.

Both are built with `createRuleParser` (`src/editor-languages/rule-parser.ts`). It takes Ace-style rules: for each state, a
list of regexes tried in order, where a rule can push or pop a state. Token names map to highlighting tags through
`TOKEN_TABLE` in the same file.

To add a token type, add it to `TOKEN_TABLE`, emit it from a rule, and give its tag a color in
`codeEditorHighlightStyle` (in `code-editor-theme.ts`).

## Styling

The theme reproduces the look of the Ace editor the console used before: Ace's `solarized_dark` theme plus the
console's overrides. Most of it is in `code-editor-theme.ts` as an `EditorView.theme(...)`, which CodeMirror scopes to
the editor and to its tooltips. Some things to know before changing it:

- **The tooltips live in one shared element at the start of `<body>`** (`getTooltipHost` in `code-editor.tsx`), so
  that dialogs, popovers and the editor's own `overflow: hidden` don't clip them, and so that it doesn't become
  `document.body.lastChild`, which the dialog specs snapshot. The comment on `getTooltipHost` has the details.

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
- **The find panel is React.** `createSearchPanel` (`search-panel.tsx`) replaces CodeMirror's own panel, which has
  its own tiny buttons, with Blueprint inputs, buttons and checkboxes so that it looks like the rest of the console. It
  renders into the panel's element with its own React root, and re-renders when the search query (or read-only state)
  changes. The search logic is still CodeMirror's (`findNext`, `replaceAll`, …) and so is the keymap: the panel runs
  the `search-panel` scope handlers on keydown, so Escape closes it, and Enter / Shift-Enter go to the next / previous
  match. Its layout is in `code-editor.scss`.
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
<div class="code-editor query-string">
  <div class="cm-editor">
    <!-- Code editor, language: hjson, value: "{\n  \"a\": 1\n}", read only -->
  </div>
</div>
```

So a snapshot changes only when the language, value, placeholder or read-only state changes, not when CodeMirror is
upgraded.

The languages are tested directly in `src/editor-languages/editor-languages.spec.ts`. It parses text with the
language's parser and checks the token name produced for each piece of text, and it checks that `dsql()` and `hjson()`
bring their completion sources.
