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

# E2E test betterment plan (temporary, delete before merging)

The e2e tests (`e2e-tests/`) were written in a Puppeteer style: Jest as the runner, the `playwright-chromium` library,
XPath selectors and element handles. Much of `util/` re-implements what Playwright now provides. This is the plan to
modernize them, in order. Each step should leave the suite passing.

## 1. Move to the `@playwright/test` runner (done)

- Add `@playwright/test` (same version as `playwright-chromium`, which stays for now because its install script
  downloads the Chromium the runner uses, so CI needs no extra `playwright install` step).
- Add `playwright.config.ts`: `testDir: 'e2e-tests'`, `workers: 1` (the specs share one cluster), the 5 minute
  timeout, `baseURL` from `DRUID_E2E_TEST_UNIFIED_CONSOLE_PORT`, `headless` from `DRUID_E2E_TEST_HEADLESS`,
  `screenshot: 'only-on-failure'`, `trace: 'retain-on-failure'`, a `globalSetup` that waits for the console.
- `npm run test-e2e` becomes `playwright test`; drop `jest.e2e.config.js`.
- Replaces: the `beforeAll`/`beforeEach`/`afterAll` boilerplate in every spec (`page` fixture),
  `util/setup.ts` (`globalSetup`), `util/debug.ts` (failure screenshots and traces), `util/retry.ts`
  (`expect.poll` / `expect(...).toPass()`), and `createBrowser`/`createPage` (the page fixture; the logging of failed
  responses moves to a fixture).
- Update `e2e-tests/README.md`, `README.md` (running e2e tests), `AGENTS.md`, `.gitignore` (`test-results/`,
  `playwright-report/` instead of `/*.jpeg`), and the eslint override.

## 2. Locators instead of XPath and element handles (done)

- `util/playwright.ts`: `getByRole('button', { name })`, labeled fields found by their `FormGroup` label rather than
  `//*[text()="..."]/following-sibling::div//input`, no `page.$` / `$$eval` / `waitForSelector` / `ElementHandle`.
- Web-first assertions (`expect(locator).toBeVisible()` / `toHaveText()`) instead of `waitForSelector`.
- `openEditActions`: find the datasource's row and click its actions button, rather than the Nth `.bp6-icon-more`.
- `selectSuggestibleInput`: no legacy `'"value"'` text selector.
- Found on the way: the Datasources and Tasks tables page at 50 rows, so on a cluster with leftover datasources the
  test's own row could be on page 2. `openView` now filters the view to the datasource through its hash route.
- Found on the way (console bugs, fixed in their own commit, could be their own PR): `TableFilters.toString()` didn't
  encode `#` or `?`, which end the hash route, and a filter value couldn't contain `|`, which separates values (so
  going from a datasource with any of those in its name to its tasks or segments filtered on the wrong thing).
  Now `#` and `?` are encoded, and `\|` / `\\` escape `|` / `\` in a value.

## 3. Don't read tables by column position (done)

- The `DatasourceColumn` / `TaskColumn` enums broke when a default visible column changed.
- The tests now wait for the cluster state through SQL (`util/sql.ts`: top-level task statuses from `sys.tasks`,
  used and available segments and rows from `sys.segments`), with `expect.poll`. Only the tutorial test still checks
  the Datasources and Tasks views, once the cluster is in the state they should show, reading the tables by their
  headers (`extractTableRecords`).
- Learned: `index_parallel` subtasks share the datasource (filter on `task_id = group_id`), `sys.segments` returns
  no row for an aggregate over no segments, and `expect.poll` stops (does not retry) when its callback throws.

## 4. Set up data through the HTTP API (done)

- `runIndexTask` shelled out to `sed` + `examples/bin/post-index-task` (bash + Python) and hard-coded the Coordinator
  on :8081. Now `readTutorialIngestionSpec` reads the spec JSON (pointing its input at this checkout), the spec sets
  the datasource name / interval on it, and `runIndexTask` `POST`s it to `/druid/indexer/v1/task` with the `request`
  fixture (so through the console's service) and polls the task status, failing with the task's error if it fails.

## 5. Clean up after each test (done)

- The `newDatasourceName(prefix)` fixture makes a unique name and, after the test passes, deletes the datasource
  (`deleteDatasource`: shut down its tasks, delete its compaction config, mark its segments unused, run a `kill` task
  and wait for it). After a failure the datasource is kept to look into.
- Found on the way (console bug, fixed in its own commit, could be its own PR): `Api.encodePath` left `^` and `|` as
  they are, which the browser sends as is and Jetty rejects (400 Illegal Path Character), so every action on a
  datasource with one of those in its name (mark unused, kill, retention rules, compaction config) failed.

## 6. Simplify the code

- Merge `component/query/overview.ts` and `component/workbench/overview.ts` (both drive `#workbench`).
- Plain object types instead of the `Object.assign(this, props)` + interface merging data classes (`PublishConfig`,
  `Datasource`, `IngestionTask`, ...); then the `no-unsafe-declaration-merging` eslint override can go.
- No `goto` + `reload({ waitUntil: 'networkidle' })`.
- `cancel-query`: one `expect(status).toBe(202)`.
- Drop `--disable-local-storage` (each test gets a fresh context anyway).

## 7. More coverage (later, separately)

- SQL-based ingestion (the SQL data loader, `INSERT` / `REPLACE` in the Query view), Explore, Supervisors,
  Segments, Services, Lookups.
