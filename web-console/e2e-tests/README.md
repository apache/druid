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
- **See the steps a test takes**: `E2E_SHOW_STEPS=true npm run test-e2e`. Each test saves a screenshot at each of
  its steps (each step of the data loader, each view it reads, a query's results, a dialog it fills in, and the
  page as it ends) to `steps/<spec>-001.png`, `-002.png`, ... (git ignored, replaced by the next run of the spec).
  The quickest way to see what a test does, especially one someone else wrote. It works on the embedded clusters
  too: `E2E_SHOW_STEPS=true mvn -pl embedded-tests verify -Pweb-console-tests ...`.

### On an embedded cluster (as in CI)

Each spec is run by a `*WebConsoleTest` class in `embedded-tests`, in the package of the functionality it covers. The
class starts an embedded cluster (in one JVM, with an in-memory metadata store, its Router on :8888) with the resources
the spec needs, and runs the spec on it with `npx playwright test <spec>`:

| Spec                                          | Run by (in `org.apache.druid.testing.embedded`) |
|-----------------------------------------------|-------------------------------------------------|
| `tutorial-batch.spec.ts`, `reindexing.spec.ts` | `indexing.BatchIndexingWebConsoleTest`          |
| `auto-compaction.spec.ts`                     | `compact.AutoCompactionWebConsoleTest`          |
| `multi-stage-query.spec.ts`                   | `msq.MultiStageQueryWebConsoleTest`             |
| `cancel-query.spec.ts`                        | `query.SqlQueryCancelWebConsoleTest`            |
| `s3-ingestion.spec.ts`                        | `s3.S3WebConsoleTest` (with an S3 container)    |
| `input-formats.spec.ts`                       | `indexer.InputFormatsWebConsoleTest`            |
| `kafka-ingestion.spec.ts`                     | `indexing.KafkaWebConsoleTest` (with Kafka)     |
| `sql-ingestion.spec.ts`                       | `msq.SqlIngestionWebConsoleTest`                |
| `datasource-actions.spec.ts`                  | `indexing.DatasourceActionsWebConsoleTest`      |
| `retention-rules.spec.ts`                     | `server.RetentionRulesWebConsoleTest`           |
| `jdbc-lookup.spec.ts`                         | `lookup.JdbcLookupWebConsoleTest`               |
| `input-formats.spec.ts`      | Loads the same 10 rows (over 2 days) of Wikipedia edits from local CSV, TSV, Parquet, ORC and Avro OCF files through the data loader, one test per format. Checks that the **Parse data** step picks the right input format by itself and parses the columns, and that the datasource ends up as 2 segments with 10 rows. Skipped without `DRUID_E2E_TEST_DATA_DIR` (the data files of embedded-tests): it runs on an embedded cluster with the Avro, Parquet and ORC extensions, from `InputFormatsWebConsoleTest`. |
| `kafka-ingestion.spec.ts`    | Follows the [Kafka tutorial](https://druid.apache.org/docs/latest/tutorials/tutorial-kafka): sets up a supervisor through the data loader's **Apache Kafka** connector on a topic with the 39,244 edits of the tutorial file (reading it from the earliest offset), and waits for the rows to be queryable. Then, from the **Supervisors** view, suspends the supervisor, resumes it and terminates it, checking its state each time (through SQL on `sys.supervisors` and in the view). Skipped without `DRUID_E2E_TEST_KAFKA_BOOTSTRAP_SERVERS` and `DRUID_E2E_TEST_KAFKA_TOPIC`: it runs on an embedded cluster with a Kafka container, from `KafkaWebConsoleTest`. |
| `sql-ingestion.spec.ts`      | SQL-based ingestion (MSQ tasks). Loads the tutorial file through the **SQL data loader** ("Load data" > "Batch - SQL": input source, parse, schema with a new datasource as the destination, ingestion progress) and waits for its 39,244 rows to be queryable. Then, in the **Query** view, ingests it with `REPLACE ... FROM TABLE(EXTERN(...))` and reindexes the 11,549 rows of `#en.wikipedia` into another datasource with `REPLACE ... FROM` the first, checking what the view says it inserted and the rows queryable each time. |
| `datasource-actions.spec.ts` | Loads `wikipedia-index.json` through the API, then from the **Datasources** view: marks all its segments unused (the datasource then shows, as "Unused", only with "Show unused"), marks them used again (the data is back, fully available), marks them unused again and deletes them with a kill task (ticking the dialog's checks of what it does), after which the kill task succeeds and the datasource is gone even with "Show unused". |
| `retention-rules.spec.ts`    | Loads `wikipedia-index.json` through the API, then edits its retention rules from the **Datasources** view (deleting the rules there, adding new ones and picking their type, then giving the reason the dialog asks for): a `loadForever` rule (as "New rule" makes it, 2 replicas), which the Retention column shows and with which the data stays loaded, then a `dropForever` rule in its place, with which the Coordinator drops the data (marking it unused). Checks the rules through the Coordinator API each time. |
| `jdbc-lookup.spec.ts`        | From the **Lookups** view, initializes the lookups (if the cluster has none) and adds a JDBC lookup (cachedNamespace, jdbc) on a table of country codes and names, checks that the view lists it, and waits for `LOOKUP('PR', ...)` and `LOOKUP('AU', ...)` to return the names. Skipped without `DRUID_E2E_TEST_LOOKUP_CONNECT_URI` and `DRUID_E2E_TEST_LOOKUP_TABLE`: it runs on an embedded cluster, with the table in its Derby metadata store, from `JdbcLookupWebConsoleTest`. |

They extend `console.WebConsoleTestBase`. There's no distribution to build and nothing left behind, as every test class
gets a new cluster. They need this checkout's Druid modules installed (`mvn install -DskipTests`), with the
`web-console` one built with the console (not with `-Dweb.console.skip=true`, which leaves the Router with no console
to serve):

```bash
# from the root of the checkout
mvn -pl web-console install -DskipTests   # after changing the console (not the tests), for the Router to serve it
mvn -pl embedded-tests verify -Pweb-console-tests                                         # all of them
mvn -pl embedded-tests verify -Pweb-console-tests -Dit.test=BatchIndexingWebConsoleTest   # one class
```

The Druid logs and the Playwright output are in `embedded-tests/target/failsafe-reports/*-output.txt`, and the
artifacts of a failed spec in `test-results/<spec>/`. The tests are tagged `web-console`, so they run only with the
`web-console-tests` profile.

- **Keep the cluster up to work on a spec**: add `-Dweb.console.keepAlive=true -Dmaven.test.redirectTestOutputToFile=false`.
  Rather than running the specs, it prints the command to run one against the cluster (with `--ui` or `--debug` added
  as you like) and waits until it's stopped.
- **Test a dev server instead of the bundled console**: add `-Dweb.console.port=18081`, with `npm start` running (it
  proxies to :8888).
- **A new spec** gets a `@Test` in the `*WebConsoleTest` class of its functionality (or a new class, next to the
  embedded tests of that functionality). `WebConsoleSpecsTest`, a unit test, fails if a spec isn't run by any. One that needs more than Druid (S3, Kafka...) adds the resource in
  `addResources` and passes its settings (endpoint, bucket...) to the spec as environment variables of `runSpec`. See
  `S3WebConsoleTest` and `s3-ingestion.spec.ts`. The resources run in Docker containers (with Testcontainers), so
  Docker has to be running.

A test deletes the datasources it created when it passes: it stops their tasks, removes their compaction config and
permanently deletes their segments (with a `kill` task). When it fails, they are kept to look into (the log says which)
and have to be deleted by hand, from the Datasources view. On an embedded cluster nothing is deleted, as the cluster is
thrown away after the test class (`DRUID_E2E_TEST_CLUSTER_IS_DISPOSABLE=true`).

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
    datasources/       Datasources view (reads the table, with unused datasources or not, runs and confirms a
                       datasource's actions, edits retention rules, edits and triggers compaction)
    lookups/           Lookups view (initializes the lookups, adds a JDBC lookup, reads the table)
    ingestion/         Tasks view (reads the table)
    load-data/         the classic data loader: loadData goes through each step with a DataLoaderConfig
                       (data-connector.ts: local disk, S3, Kafka, reindex from Druid; partitions-spec.ts: hashed,
                       range); sql-data-loader.ts: the SQL data loader (loadDataWithSql)
    supervisors/       Supervisors view (reads the table, runs and confirms a supervisor's actions)
    workbench/         Query view: run a query or an ingestion query (accepting the task slot warning), cancel a
                       query
  util/
    fixtures.ts        the `test` and `expect` to import in specs (`test` logs failed responses, shows the last
                       step, and has the newDatasourceName fixture)
    global-setup.ts    waits for the console's SQL endpoint before the tests start
    druid.ts           the tutorial data dir and ingestion specs, runTask (submits a task and waits for it),
                       deleteDatasource
    sql.ts             querySql, and the cluster state the tests wait for: task statuses, a datasource's segments
                       and queryable rows, a supervisor's state
    playwright.ts      openView, and helpers that find inputs and buttons by their label or text
    table.ts           extractTable / extractTableRecords: read a table's rows as text (by position / by header)
    steps.ts           showStep: a screenshot of a step of the test, with E2E_SHOW_STEPS=true
  steps/               the screenshots of showStep (git ignored)
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
  fail fast (like `WorkbenchOverview.runQuery` throwing on a query error) rather than wait out a timeout.
- Set up data that the test is not about through the API, not the UI: `readTutorialIngestionSpec`, change what you
  need (the datasource name at least) and `runTask(request, ingestionSpec)`.
- Call `showStep(page, 'what the page shows')` at the points worth seeing, once the page shows them (in a page
  object, so every test using it gets the step). With `E2E_SHOW_STEPS=true` it saves a screenshot, otherwise it does
  nothing.
- Name a datasource you create with the `newDatasourceName` fixture: `newDatasourceName('my-test')` makes a unique
  name (the prefix and the time) and deletes the datasource after the test passes.
