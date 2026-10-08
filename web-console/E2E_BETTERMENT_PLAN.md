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
  test's own row could be on page 2. `openView` now filters the view through its hash route. A filter can't hold
  `|`, `#` or `?`, so it matches on the end of the name after the last of those (`filterableText`).
- Found on the way (console bug, not fixed here): `TableFilters.toString()` doesn't encode `#` or `?`, but
  `parseHashRoute` ends the route at them, so going from a datasource whose name has `#` or `?` to its tasks or
  segments drops the rest of the name.

## 3. Don't read tables by column position

- The `DatasourceColumn` / `TaskColumn` enums break when a default visible column changes. Check cluster state
  (task succeeded, N segments available, row count) through the SQL API (`sys.tasks`, `sys.segments`) where the UI
  isn't what's being tested, or find columns by their header text.

## 4. Set up data through the HTTP API

- `runIndexTask` shells out to `sed` + `examples/bin/post-index-task` (bash + Python) and hard-codes the Coordinator
  on :8081. Read the spec JSON, set the datasource name / interval in code and `POST` it to `/druid/indexer/v1/task`
  with the `request` fixture, then poll the task status.

## 5. Clean up after each test

- Mark the datasources a test created as unused (and kill its tasks) in an `afterEach`/fixture teardown.

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
