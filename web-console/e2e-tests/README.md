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

# End-to-end tests

These tests drive the web console in a real (headless) Chromium against a real Druid cluster. They catch what the unit
tests can't: the console and Druid's APIs disagreeing, a flow across several views breaking, or a request that only
fails against a live cluster.

They use the Playwright test runner (`@playwright/test`, configured in `../playwright.config.ts`). The Chromium it
drives is downloaded by the `playwright-chromium` package's install script, so keep the two packages on the same
version. They run in CI on embedded clusters (see below), as part of `.github/scripts/web-checks.sh`.

## Running them

The tests need a Druid cluster with its Router on :8888 (every request goes through the console's service), running
on the same machine (its tasks read the `examples/quickstart/tutorial` data files of this checkout):

```bash
script/druid build   # once, builds a distribution with the extensions the tests need
script/druid start
npm run test-e2e     # all the tests, one at a time (they share the cluster)
script/druid stop
```

Before the tests start, a global setup (`util/global-setup.ts`) waits for the console's SQL endpoint to answer.

- **One file**: `npm run test-e2e -- cancel-query` (any `playwright test` arguments go after `--`)
- **See the browser**: `npm run test-e2e -- --headed`, step through a test with `--debug`, or pick and watch tests in
  the UI mode with `--ui`
- **Test a dev server instead of the bundled console**: `DRUID_E2E_TEST_UNIFIED_CONSOLE_PORT=18081 npm run test-e2e`.
  Without it the tests run against the console that the router on :8888 serves, which was bundled when Druid was
  built, not your working tree.
- **On failure** a test leaves a screenshot, a trace (`npx playwright show-trace <path>`: every action with the DOM,
  console and network at that point) and an `error-context.md` (the page as an accessibility tree) in `test-results/`
  (git ignored). Every HTTP response with a status of 400 or more is also logged with its body
  (`util/fixtures.ts`).

### On an embedded cluster (as in CI)

`CoreWebConsoleTest` in `embedded-tests` (package `org.apache.druid.testing.embedded.console`) starts an embedded
cluster (in one JVM, with an in-memory metadata store, its Router on :8888) and runs each spec on it with
`npx playwright test <spec>`. There's no distribution to build and nothing left behind, as every test class gets a new
cluster. It needs this checkout's Druid modules installed (`mvn install -DskipTests`), with the `web-console` one built
with the console (not with `-Dweb.console.skip=true`, which leaves the Router with no console to serve):

```bash
# from the root of the checkout
mvn -pl web-console install -DskipTests   # after changing the console (not the tests), for the Router to serve it
mvn -pl embedded-tests verify -Pweb-console-tests -Dit.test=CoreWebConsoleTest
```

The Druid logs and the Playwright output are in `embedded-tests/target/failsafe-reports/*-output.txt`, and the
artifacts of a failed spec in `test-results/<spec>/`. The tests are tagged `web-console`, so they run only with the
`web-console-tests` profile.

- **Keep the cluster up to work on a spec**: add `-Dweb.console.keepAlive=true -Dmaven.test.redirectTestOutputToFile=false`.
  Rather than running the specs, it prints the command to run one against the cluster (with `--ui` or `--debug` added
  as you like) and waits until it's stopped.
- **Test a dev server instead of the bundled console**: add `-Dweb.console.port=18081`, with `npm start` running (it
  proxies to :8888).
- **A spec that needs more than Druid** (S3, Kafka...) gets its own `*WebConsoleTest` class that extends
  `WebConsoleTestBase`, adds the resource in `addResources` and passes its settings (endpoint, bucket...) to the spec as
  environment variables of `runSpec`. See `S3WebConsoleTest` and `s3-ingestion.spec.ts`. The resources run in Docker
  containers (with Testcontainers), so Docker has to be running.

A test deletes the datasources it created when it passes: it stops their tasks, removes their compaction config and
permanently deletes their segments (with a `kill` task). When it fails, they are kept to look into (the log says which)
and have to be deleted by hand, from the Datasources view.

## The tests

Each test has a 5 minute timeout. Polling for cluster state (a task finishing, segments loading) retries every second
for up to 2 minutes.

| Spec                         | What it does                                                                                                                                                                                                                                                                                                                                                                                         |
|------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `tutorial-batch.spec.ts`     | Follows the [batch loading tutorial](https://druid.apache.org/docs/latest/tutorials/tutorial-batch) through the classic **data loader**: connects to `wikiticker-2015-09-12-sampled.json.gz` on local disk, checks the first and last preview lines, sets the timestamp to `timestamp_parse("time") + 1` (so the `__time` check below proves the expression was used), turns rollup off, sets DAY granularity and submits. It then waits (through SQL on `sys.tasks` and `sys.segments`) for the task to succeed and for the datasource to be fully available as 1 segment with 39,244 rows, checks that the **Tasks** and **Datasources** views show the same, and checks the first row of `SELECT *` in the **Query** view. The datasource name contains a string of special and non-Latin characters to test quoting and escaping. |
| `reindexing.spec.ts`         | Loads `wikipedia-index.json` through the API (1 segment), then reindexes it through the data loader's **Reindex from Druid** connector into range partitions on `channel` with 10,000 target rows per segment. Checks the preview rows, that the task succeeds, and that the datasource ends up as 4 segments with the same 39,244 rows.                                                         |
| `auto-compaction.spec.ts`    | Follows the [compaction tutorial](https://druid.apache.org/docs/latest/tutorials/tutorial-compaction): loads 2 hours of `compaction-init-index.json` through the API (3 segments, 1,412 rows), sets a compaction config (`skipOffsetFromLatest: PT0S`, hashed partitions) from the **Datasources** view, reopens the dialog until it reads back the same config, then forces compaction runs (from the Alt-click "more" menu) until there are 2 segments. |
| `multi-stage-query.spec.ts`  | Runs an MSQ `SELECT` over `EXTERN(...)` on the tutorial file in the **Query** view, clicking "Run it anyway" if the cluster warns it lacks task slots, and checks the top 2 of the 10 channels by count.                                                                                                                                                                                             |
| `cancel-query.spec.ts`       | Runs `SELECT sleep(40)` in the **Query** view, waits for its `POST` to `druid/v2`, clicks "Cancel query" and checks that the `DELETE` it sends is answered with `202 Accepted`.                                                                                                                                                                                                                       |
| `s3-ingestion.spec.ts`       | Loads the tutorial file from S3 through the data loader's **Amazon S3** connector (its URI from `DRUID_E2E_TEST_S3_URI`) and waits for the task to succeed and for 1 segment with 39,244 rows. Skipped without `DRUID_E2E_TEST_S3_URI`: it runs on an embedded cluster with an S3 container, from `S3WebConsoleTest` (which also makes S3 the deep storage and task log storage). |

## Layout

```
e2e-tests/
  *.spec.ts            the tests, one describe per file
  component/           page objects: one per view, wrapping its selectors
    datasources/       Datasources view (reads the table, edits and triggers compaction)
    ingestion/         Tasks view (reads the table)
    load-data/         the classic data loader: loadData goes through each step with a DataLoaderConfig
                       (data-connector.ts: local disk, reindex from Druid; partitions-spec.ts: hashed, range)
    workbench/         Query view: run a query (accepting the task slot warning), cancel a query
  util/
    fixtures.ts        the `test` and `expect` to import in specs (`test` logs failed responses, and has the
                       newDatasourceName fixture)
    global-setup.ts    waits for the console's SQL endpoint before the tests start
    druid.ts           the tutorial data dir and ingestion specs, runTask (submits a task and waits for it),
                       deleteDatasource
    sql.ts             querySql, and the cluster state the tests wait for: task statuses, a datasource's segments
    playwright.ts      openView, and helpers that find inputs and buttons by their label or text
    table.ts           extractTable / extractTableRecords: read a table's rows as text (by position / by header)
```

### Writing a test

- Import `test` and `expect` from `util/fixtures.ts` (not from `@playwright/test`) and take the `page` fixture:
  `test('...', async ({ page }) => { ... })`. Each test gets a fresh browser context, so no local storage carries
  over.
- Put selectors in a page object under `component/` rather than in the spec (and keep data, like what to set in a
  form, as plain objects, with a union type when it comes in kinds), and use locators (`page.locator`,
  `getByRole`), not `page.$` / `waitForSelector` / XPath. The helpers in `util/playwright.ts` find form fields by
  their visible label (`setLabeledInput(page, 'Datasource name', ...)`; the labels are not linked to their inputs,
  so `getByLabel` does not work) and buttons by their exact text, so renaming a label or a button in the console
  breaks the tests. Scope a locator (to a dialog, `.next-bar`, ...) when the same text appears twice.
- Open a view with `openView(page, 'datasources', { datasource: ... })`. Filtering to the test's own rows keeps the
  test working on a cluster with more datasources or tasks than fit on one page.
- The query editor is CodeMirror; type into it with `setQueryInput`, which fills its `.cm-content`.
- Wait for the cluster state (a task finishing, segments loading) through SQL rather than by reading a view:
  `await expect.poll(() => getDatasourceSegments(request, name), CLUSTER_STATE_POLL).toEqual({ ... })`, with the
  `request` fixture. Check a view only when the view is what you are testing, and once the cluster is in the state
  it should show.
- Read the console's tables with `extractTableRecords`, which keys each row by its column headers, so a column
  being added, hidden or moved doesn't break the test.
- Retry UI checks that depend on data loading with `await expect(async () => { ... }).toPass()`. Make each attempt
  fail fast (like `QueryOverview.runQuery` throwing on a query error) rather than wait out a timeout.
- Set up data that the test is not about through the API, not the UI: `readTutorialIngestionSpec`, change what you
  need (the datasource name at least) and `runTask(request, ingestionSpec)`.
- Name a datasource you create with the `newDatasourceName` fixture: `newDatasourceName('my-test')` makes a unique
  name (the prefix and the time) and deletes the datasource after the test passes.
