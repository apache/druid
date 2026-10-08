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

## 6. Simplify the code (done)

- Merged `component/query/overview.ts` into `component/workbench/overview.ts` (both drove `#workbench`).
- Plain object types instead of the `Object.assign(this, props)` + interface merging data classes, and union types
  with functions for the partitions specs and data connectors; the data loader is a `loadData(page, config)`
  function with one flat config. The `no-unsafe-declaration-merging` eslint override is gone.
- Done earlier, in steps 1-2: no `goto` + `reload({ waitUntil: 'networkidle' })`, one `expect` in `cancel-query`,
  no `--disable-local-storage`.

## 7. More coverage (later, separately)

- SQL-based ingestion (the SQL data loader, `INSERT` / `REPLACE` in the Query view), Explore, Supervisors,
  Segments, Services, Lookups.

## 8. Run on embedded clusters (prototype done)

Prototype (option B: JUnit drives, the specs stay in TypeScript): `WebConsoleTestBase` and `CoreWebConsoleTest` in
`embedded-tests` (`org.apache.druid.testing.embedded.console`), tag `web-console`, profile `web-console-tests`. The 5
specs pass unchanged on it (50s for the whole Maven run), with a keep-alive mode for working on a spec.
`S3WebConsoleTest` (with `S3StorageResource`) runs `s3-ingestion.spec.ts`, which loads from `s3://` through the data
loader (24s for the test class).

CI: `web-checks.sh` installs `web-console` (with the console built), then runs
`mvn verify -pl 'embedded-tests,!web-console' -am -Pweb-console-tests -DskipUTs`, which builds the 38 modules
embedded-tests needs (not the whole of Druid, nor a distribution). `worker.yml` uploads
`embedded-tests/target/failsafe-reports/` (the Druid and Playwright logs) and `web-console/test-results/` (instead of
the distribution's logs), and the JUnit report shows the specs' results. `script/druid` stays for running the tests
against a quickstart locally.

Found on the way (fixed in the specs, they were latent races that the quickstart's timing hid):
- `extractTableRecords` read a view's table right after opening it, before the view loaded its data.
- `cancelQuery` waited for any SQL `POST` (it could be the column tree's) and canceled before the query was sent.

Left:
- Make the `newDatasourceName` cleanup optional (a new cluster per class doesn't need it), or drop it with the
  quickstart way.
- A check that every spec is run by some `*WebConsoleTest` (they are listed by hand).
- The JUnit report names the cases `testSpec(String)[1]`..., not by spec.
