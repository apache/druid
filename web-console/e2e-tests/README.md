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

They use Jest as the test runner and the `playwright-chromium` library to control the browser (not the
`@playwright/test` runner). They run in CI as part of `.github/scripts/web-checks.sh`.

## Running them

The tests need a Druid cluster on the standard quickstart ports, started from this checkout (they read the
`examples/quickstart/tutorial` data files from it and post tasks with `examples/bin/post-index-task`):

```bash
script/druid build   # once, builds a distribution with the extensions the tests need
script/druid start
npm run test-e2e     # all the tests, one file at a time (--runInBand)
script/druid stop
```

- **One file**: `npx jest --config jest.e2e.config.js e2e-tests/cancel-query.spec.ts`
- **See the browser**: `DRUID_E2E_TEST_HEADLESS=false npm run test-e2e` (also slows each action down by 20ms)
- **Test a dev server instead of the bundled console**: `DRUID_E2E_TEST_UNIFIED_CONSOLE_PORT=18081 npm run test-e2e`.
  Without it the tests run against the console that the router on :8888 serves, which was bundled when Druid was
  built, not your working tree.
- **On failure** a test writes `<test-name>-error-screenshot.jpeg` to the directory Jest was run from (git ignored),
  and prints it to the log as a base64 data URL so it can be recovered from CI logs. Every HTTP response with a status
  of 400 or more is also logged with its body.

The tests create datasources and tasks and do not clean them up. Each datasource name ends in a timestamp so that
runs don't collide, but a cluster that runs the tests often will collect them.

## The tests

Each test has a 5 minute timeout. Polling for cluster state (a task finishing, segments loading) retries for up to a
minute.

| Spec                         | What it does                                                                                                                                                                                                                                                                                                                                                                                         |
|------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `tutorial-batch.spec.ts`     | Follows the [batch loading tutorial](https://druid.apache.org/docs/latest/tutorials/tutorial-batch) through the classic **data loader**: connects to `wikiticker-2015-09-12-sampled.json.gz` on local disk, checks the first and last preview lines, sets the timestamp to `timestamp_parse("time") + 1` (so the `__time` check below proves the expression was used), turns rollup off, sets DAY granularity and submits. It then waits for the task to succeed on the **Tasks** view, for the datasource to be fully available as 1 segment with 39,244 rows on the **Datasources** view, and checks the first row of `SELECT *` in the **Query** view. The datasource name contains a string of special and non-Latin characters to test quoting and escaping. |
| `reindexing.spec.ts`         | Loads `wikipedia-index.json` with `post-index-task` (1 segment), then reindexes it through the data loader's **Reindex from Druid** connector into range partitions on `channel` with 10,000 target rows per segment. Checks the preview rows, that the task succeeds, and that the datasource ends up as 4 segments with the same 39,244 rows.                                                         |
| `auto-compaction.spec.ts`    | Follows the [compaction tutorial](https://druid.apache.org/docs/latest/tutorials/tutorial-compaction): loads 2 hours of `compaction-init-index.json` with `post-index-task` (3 segments, 1,412 rows), sets a compaction config (`skipOffsetFromLatest: PT0S`, hashed partitions) from the **Datasources** view, reopens the dialog until it reads back the same config, then forces compaction runs (from the Alt-click "more" menu) until there are 2 segments. |
| `multi-stage-query.spec.ts`  | Runs an MSQ `SELECT` over `EXTERN(...)` on the tutorial file in the **Query** view, clicking "Run it anyway" if the cluster warns it lacks task slots, and checks the top 2 of the 10 channels by count.                                                                                                                                                                                             |
| `cancel-query.spec.ts`       | Runs `SELECT sleep(40)` in the **Query** view, waits for its `POST` to `druid/v2`, clicks "Cancel query" and checks that the `DELETE` it sends is answered with `202 Accepted`.                                                                                                                                                                                                                       |

## Layout

```
e2e-tests/
  *.spec.ts            the tests, one describe per file
  component/           page objects: one class per view, wrapping its selectors
    datasources/       Datasources view (reads the table, edits and triggers compaction)
    ingestion/         Tasks view (reads the table)
    load-data/         the classic data loader, step by step
      data-connector/  the input sources it can connect to (local disk, reindex from Druid)
      config/          what to set at each step (timestamp, schema, partition, publish)
    query/             Query view: run a query, cancel a query
    workbench/         Query view: run a query, accepting the task slot warning
  util/
    druid.ts           console URL (from DRUID_E2E_TEST_UNIFIED_CONSOLE_PORT), tutorial data dir, runIndexTask
    playwright.ts      browser and page setup, and helpers that find inputs and buttons by their label or text
    table.ts           extractTable: reads a table's rows as text, skipping blank rows
    retry.ts           retry a callback on a failed expect (or on any error) every second
    debug.ts           saveScreenshotIfError
    setup.ts           waitTillWebConsoleReady: waits for the console to stop showing its "cannot connect" message
```

### Writing a test

- Start from an existing spec: `beforeAll` waits for the console and launches the browser, `beforeEach` opens a fresh
  page, and the body goes inside `saveScreenshotIfError(testName, page, ...)`.
- Put selectors in a page object under `component/` rather than in the spec. The helpers in `util/playwright.ts` find
  form fields by their visible label (`setLabeledInput(page, 'Datasource name', ...)`) and buttons by their text, so
  renaming a label or a button in the console breaks the tests.
- The query editor is CodeMirror; type into it with `setQueryInput`, which fills its `.cm-content`.
- Table helpers read columns by position (see the column enums in `component/datasources/overview.ts` and
  `component/ingestion/overview.ts`), so adding, removing or reordering a default visible column needs the enum
  updated.
- Wrap checks on cluster state in `retryIfJestAssertionError`: segments load and tasks finish some time after the
  console reports success.
- Give anything you create a unique name (append `new Date().toISOString()`).
