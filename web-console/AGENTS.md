# AGENTS.md

The Apache Druid web console: a React + TypeScript single page app built with Blueprint.js, served by the Druid router.
`README.md` has the setup and the directory layout.

## Commands

- **Node** is pinned in `.node-version` and installed with mise (`mise install`). Don't change the global Node version.
- **Generated sources**: `src/editor-languages/dsql-docs.ts` and `src/editor-languages/*.parser*.ts` are generated and gitignored.
  `npm run compile` (`script/build`) makes them, and only rebuilds what is out of date. Run it after a fresh clone, after
  changing a `.grammar` file or the SQL docs, or when typecheck can't find those files. Without
  `public/web-console-<version>.js` it also builds the whole console bundle, which is slow.
- **Dev server**: `npm start` serves on http://localhost:18081 and proxies API calls to Druid on `localhost:8888`
  (`druid_host=host:port npm start` for another one). A dev server may already be running there, so check the port
  before starting one. Views are hash routes, like `/unified-console.html#workbench`.
- **Checks**:
  - `npx jest src/path/to/file.spec.ts` runs some specs (add `-u` to update their snapshots), `npm run jest` all of
    them.
  - `npm run typecheck`, `npm run eslint`, `npm run sasslint`, `npm run prettify-check`.
  - `npm run test-unit` runs everything above (and the generators) without building the bundle. `npm test` builds the
    bundle first.
  - `npm run autofix` fixes ESLint, stylelint and Prettier issues.
- **End-to-end tests** (`e2e-tests/`, Playwright) need a running Druid: `script/druid build && script/druid start`,
  then `npm run test-e2e`. See the README for running one test or with a visible browser.

## Where things are

- `src/views/` - one folder per top-level view (`workbench-view` is the SQL editor, `explore-view`,
  `load-data-view`, `datasources-view`, ...), `src/dialogs/`, `src/components/` - the UI.
- `src/druid-models/` - TypeScript models of Druid concepts (ingestion specs, tasks, queries) and their `AutoForm`
  field definitions and JSON completions.
- `src/singletons/` - `Api.instance` (the axios client for Druid, which turns error responses into readable
  messages), `AppToaster`, caches. `src/helpers/capabilities.ts` - what the cluster supports (SQL, proxy, ...).
- `src/utils/` - general helpers, `QueryManager` (async work with loading, error and cancellation states; use it from
  components with `useQueryManager` in `src/hooks/`), hash routing, local storage keys.
- `src/components/code-editor/` - the CodeMirror based `CodeEditor`. Read its `README.md` before changing the editor,
  the languages (`src/editor-languages/`: Lezer grammars for DruidSQL and Hjson) or the completions
  (`src/editor-completions/`).
- SQL is built and parsed with `druid-query-toolkit` (`SqlQuery`, `C`, `F`, `T`, `L`, `N`, ...) rather than with
  strings.

## Conventions

- **License header**: every source file (`.ts`, `.tsx`, `.scss`, `.mjs`, and `.md` as an HTML comment) starts with the
  Apache 2.0 header; copy it from a neighboring file of the same type. ESLint checks it in TypeScript files.
- **Components** live in `component-name/component-name.tsx` with their `.scss` and `.spec.tsx` next to them, and are
  exported from `src/components/index.ts` (likewise for dialogs, views, utils, hooks, druid-models). Import from those
  barrels (`../../components`, `../../utils`).
- **Snapshots**: many specs snapshot the DOM. The serializer (`src/test-utils/snapshot-serializer.ts`) reduces icons to
  their name and each code editor to a comment (language, value, placeholder), so only real changes show up.
- **Forms** for Druid specs are usually an `AutoForm` driven by `Field` definitions in `src/druid-models/`, not
  hand-written inputs.
- **Styling**: Blueprint components first. In SCSS, prefer the variables (`@use '../../variables' as *`, which has the
  Blueprint ones) over literal colors and sizes.

### Tooltips: use `data-tooltip`
For a plain text hint on hover, put a `data-tooltip` attribute on the element. This is the preferred way to add a
tooltip in the console, use it rather than Blueprint's `Tooltip` or a `title` attribute.
- It shows instantly and follows the mouse. `initMouseTooltip` (`src/utils/mouse-tooltip/`, called once in
  `src/entry.tsx`) listens on the document and shows the `data-tooltip` of the nearest element under the mouse (or its
  closest ancestor that has one), so nothing needs to be wrapped or imported.
- `\n` makes a new line (the tooltip is `white-space: pre-wrap`); `undefined` (or an empty string) means no tooltip.
- It works on anything that ends up in the DOM: plain elements, Blueprint components that pass props through
  (`<Button icon={IconNames.MORE} data-tooltip="More column options" />`, `<Icon ... data-tooltip="Experimental" />`),
  and DOM that React does not render, like CodeMirror decorations (`attributes: { 'data-tooltip': message }` in
  `src/components/code-editor/error-mark.ts`).
- Examples: `click-to-copy.tsx` (`` data-tooltip={`Click to copy:\n${text}`} ``), `segment-timeline.tsx`,
  `execution-stages-pane.tsx`, `resource-pane.tsx`, `header-bar.tsx`, `filter-pane.tsx`.
- Use a Blueprint `Popover` (not a tooltip) only when the content is rich or interactive.
