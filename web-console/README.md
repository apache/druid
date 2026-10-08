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

# Apache Druid web console

This is the Druid web console that serves as a data management interface for Druid.

## Developing the console

### Getting started

1. You need to be within the `web-console` directory
2. Install [mise](https://mise.jdx.dev/getting-started.html) and run `mise trust` and `mise install` to get the correct Node.js version (pinned in `.node-version`)
3. Install the modules with `npm install`
4. Run `npm run compile` to compile the SCSS files (this usually needs to be done only once)
5. Run `npm start` to start in development mode and proxy Druid requests to `localhost:8888`

**Note:** you can provide an environment variable to proxy to a different Druid host like so: `druid_host=1.2.3.4:8888 npm start`
**Note:** you can provide an environment variable to use webpack-bundle-analyzer as a plugin in the build script like so: `BUNDLE_ANALYZER_PLUGIN='TRUE' npm start`

To try the console in (say) coordinator mode you could run it as such:

`druid_host=localhost:8081 npm start`

### Developing

You should use a TypeScript friendly IDE (such as [WebStorm](https://www.jetbrains.com/webstorm/), or [VS Code](https://code.visualstudio.com/)) to develop the web console.

The console relies on [eslint](https://eslint.org) (and various plugins), [sass-lint](https://github.com/sasstools/sass-lint), and [prettier](https://prettier.io/) to enforce code style. If you are going to do any non-trivial development you should set up your IDE to automatically lint and fix your code as you make changes.

#### Updating dependencies due to CVEs

Sometimes a scanner flags an issue with a dependency of the console, if you want to fix it with minimal effort, follow these steps:

1. From the web-console directory run `npm audit` to get a list of dependencies with issues as reported by npm. Make sure that whatever CVE leads you to follow these steps is also being flagged by npm (it almost always is).
2. Run `npm audit --fix` to fix the issues, npm will bump the dependencies minimally.
3. Run `script/licenses` to update the Druid license file with the changes from the audit fix command.
4. Bonus points: start the console with `npm start` and make sure it loads and basically works. CI tests will also do that but it's good practice to verify yourself first.
5. Commit the changes in a branch and make a pull request.

#### Configuring WebStorm

- **Preferences | Languages & Frameworks | JavaScript | Code Quality Tools | ESLint**

  - Select "Automatic ESLint Configuration"
  - Check "Run eslint --fix on save"

- **Preferences | Languages & Frameworks | JavaScript | Prettier**
  - Set "Run for files" to `{**/*,*}.{js,ts,jsx,tsx,css,scss}`
  - Check "On code reformat"
  - Check "On save"

#### Configuring VS Code

- Install `dbaeumer.vscode-eslint` extension
- Install `esbenp.prettier-vscode` extension
- Select `Open User Settings (JSON)` from the editor commands (`Ctrl+Shift+P` or `Command+Shift+P`) and set the following:

  ```json
    "editor.defaultFormatter": "esbenp.prettier-vscode",
    "editor.formatOnSave": true,
    "editor.codeActionsOnSave": {
      "source.fixAll.eslint": true
    }
  ```

#### Auto-fixing manually

It is also possible to auto-fix and format code without making IDE changes by running the following script:

- `npm run autofix` &mdash; run code linters and formatter

You could also run fixers individually:

- `npm run eslint-fix` &mdash; run code linter and fix issues
- `npm run sasslint-fix` &mdash; run style linter and fix issues
- `npm run prettify` &mdash; reformat code and styles

### Updating the list of license files

If you change the dependencies of the console in any way, please run `script/licenses` (from the web-console directory).
It will analyze the changes and update the `../licenses` file as needed.

Please be conscious of not introducing dependencies on packages with Apache incompatible licenses.

### Running end-to-end tests

From the web-console directory:

1. Build druid distribution: `script/druid build`
2. Start druid cluster: `script/druid start`
3. Run end-to-end tests: `npm run test-e2e`
4. Stop druid cluster: `script/druid stop`

If you already have a druid cluster running on the standard ports, the steps to build/start/stop a druid cluster can
be skipped.

#### Screenshots for debugging

`e2e-tests/util/debug.ts:saveScreenshotIfError()` is used to save a screenshot of the web console
when the test fails. For example, if `e2e-tests/tutorial-batch.spec.ts` fails, it will create
`load-data-from-local-disk-error-screenshot.jpeg`.

#### Disabling headless mode

Disabling headless mode while running the tests can be helpful. This can be done via the `DRUID_E2E_TEST_HEADLESS`
environment variable, which defaults to `true`.

Like so: `DRUID_E2E_TEST_HEADLESS=false npm run test-e2e`

#### Running against alternate web console

The environment variable `DRUID_E2E_TEST_UNIFIED_CONSOLE_PORT` can be used to target a web console running on a
non-default port (i.e., not port `8888`). For example, this environment variable can be used to target the
development mode of the web console (started via `npm start`), which runs on port `18081`.

Like so: `DRUID_E2E_TEST_UNIFIED_CONSOLE_PORT=18081 npm run test-e2e`

#### Running and debugging a single e2e test using Jest and Playwright

- Run - `jest --config jest.e2e.config.js e2e-tests/tutorial-batch.spec.ts`
- Debug - `PWDEBUG=console jest --config jest.e2e.config.js e2e-tests/tutorial-batch.spec.ts`

## Description of the directory structure

As part of this directory:

- `assets/` - The images (and other assets) used within the console
- `e2e-tests/` - End-to-end tests for the console (see `e2e-tests/README.md`)
- `public/` - The compiled destination for the files powering this console
- `script/` - Some helper bash scripts for running this console
- `src/` - This directory constitutes all the source code for this console

## List of APIs used

All requests are made relative to the service the console is served from (normally the Router, which forwards
`/druid/coordinator/*` and `/druid/indexer/*` to the Coordinator and Overlord via the management proxy).
Path parameters are shown in `{braces}`.

### APIs used in normal operation

#### Capability detection (on console load)

| API                                                   | Used for                                                                   | Where       |
|-------------------------------------------------------|----------------------------------------------------------------------------|-------------|
| `POST /druid/v2/sql?capabilities`                     | Probing whether the SQL endpoint is available                              | Console load |
| `GET /status?capabilities`                            | Probing that the service is reachable at all (when the SQL probe fails)    | Console load |
| `POST /druid/v2?capabilities`                         | Probing whether native queries work when SQL does not                      | Console load |
| `GET /proxy/enabled?capabilities`                     | Detecting whether the Router management proxy is enabled                   | Console load |
| `GET /druid/coordinator/v1/isLeader?capabilities`     | Detecting a Coordinator when the console is not served from the Router     | Console load |
| `GET /druid/indexer/v1/isLeader?capabilities`         | Detecting an Overlord when the console is not served from the Router       | Console load |
| `GET /druid/v2/sql/task/enabled?capabilities`         | Detecting whether the MSQ task engine is available                         | Console load |
| `GET /druid/v2/sql/engines?capabilities`              | Detecting whether the MSQ Dart engine is available                         | Console load |
| `POST /druid/v2/sql?capabilities-functions`           | Listing available SQL functions (for autocomplete and docs)                | Console load |
| `GET /druid/indexer/v1/totalWorkerCapacity`           | Cluster task slot capacity                                                 | Console load, Home (Tasks card), Query view and SQL data loader (stuck-stage warnings) |

#### Status and cluster health

| API                                          | Used for                                                    | Where                                  |
|----------------------------------------------|-------------------------------------------------------------|----------------------------------------|
| `GET /status`                                | Druid version and loaded extensions                         | Home (Status card and Status dialog), Query view (query detail archive download) |
| `GET /status/properties`                     | Runtime properties of the service the console is served from | Doctor dialog                          |
| `GET /proxy/coordinator/status`              | Coordinator version check                                   | Doctor dialog                          |
| `GET /proxy/coordinator/status/properties`   | Coordinator runtime properties check                        | Doctor dialog                          |
| `GET /proxy/overlord/status`                 | Overlord version check; list of extensions loaded on the Overlord | Doctor dialog, Load data view       |
| `GET /proxy/overlord/status/properties`      | Overlord runtime properties check                           | Doctor dialog                          |

#### Queries

| API                                                         | Used for                                                                     | Where |
|-------------------------------------------------------------|------------------------------------------------------------------------------|-------|
| `POST /druid/v2/sql`                                        | Running SQL queries (including all `sys.*` table queries that power the views) | Everywhere: Query view, Explore view, all table views and Home cards, preview panes, Doctor dialog |
| `POST /druid/v2`                                            | Running native queries                                                       | Query view (native engine), Load data view (reindexing from a Druid datasource) |
| `DELETE /druid/v2/sql/{queryId}`                            | Cancelling a running SQL query                                               | Query view (native and Dart engines, current Dart queries panel) |
| `DELETE /druid/v2/{queryId}`                                | Cancelling a running native query                                            | Query view |
| `GET /druid/v2/sql/queries?includeComplete`                 | Listing current and recent Dart queries                                      | Query view (current Dart queries panel) |
| `GET /druid/v2/sql/queries/{queryId}/reports`               | Query report / execution details of a Dart query                             | Query view |
| `POST /druid/v2/sql/task`                                   | Explaining an MSQ query                                                      | Query view (Explain dialog) |
| `POST /druid/v2/sql/statements`                             | Submitting an MSQ task query                                                 | Query view, SQL data loader |
| `GET /druid/v2/sql/statements/{queryId}`                    | Status, detail, and result pages of an MSQ query                             | Query view, SQL data loader |
| `GET /druid/v2/sql/statements/{queryId}/results`            | Downloading the results of an MSQ query                                      | Query view (destination pages) |
| `GET /druid/indexer/v1/task/{taskId}/reports`               | Live reports (stages, counters) of an MSQ task                               | Query view, SQL data loader |
| `GET /druid/indexer/v1/task/{taskId}`                       | Payload of an MSQ task                                                       | Query view, SQL data loader |
| `POST /druid/indexer/v1/task/{taskId}/shutdown`             | Cancelling an MSQ task query                                                 | Query view, SQL data loader |

#### Datasources, segments, retention, and compaction

| API                                                                       | Used for                                                     | Where |
|---------------------------------------------------------------------------|--------------------------------------------------------------|-------|
| `GET /druid/coordinator/v1/datasources`                                   | Checking which datasources already exist                     | Load data view |
| `GET /druid/coordinator/v1/metadata/datasources?includeUnused`            | Listing datasources that only have unused segments           | Datasources view ("Show unused") |
| `GET /druid/coordinator/v1/rules`                                         | Load rules for all datasources (and the cluster defaults)    | Datasources view (Retention column), Segment timeline (Datasources and Segments views) |
| `POST /druid/coordinator/v1/rules/{datasource}`                           | Saving the load rules of a datasource                        | Datasources view (Retention dialog) |
| `GET /druid/coordinator/v1/rules/{datasource}/history?count=200`          | Load rule change history                                     | Datasources view (Retention dialog) |
| `DELETE /druid/coordinator/v1/datasources/{datasource}/intervals/{interval}` | Permanently deleting (killing) unused segments            | Datasources view (Kill dialog) |
| `POST /druid/indexer/v1/datasources/{datasource}`                         | Marking all segments of a datasource as used                 | Datasources view |
| `DELETE /druid/indexer/v1/datasources/{datasource}`                       | Marking all segments of a datasource as unused               | Datasources view |
| `POST /druid/indexer/v1/datasources/{datasource}/markUsed`                | Marking segments in an interval as used                      | Datasources view |
| `POST /druid/indexer/v1/datasources/{datasource}/markUnused`              | Marking segments in an interval as unused                    | Datasources view |
| `DELETE /druid/indexer/v1/datasources/{datasource}/segments/{segmentId}`  | Marking a single segment as unused (drop)                    | Segments view |
| `GET /druid/coordinator/v1/metadata/datasources/{datasource}/segments/{segmentId}` | Segment metadata                                    | Segments view (segment detail dialog) |
| `GET /druid/indexer/v1/compaction/config/datasources`                     | Auto-compaction configs of all datasources                   | Datasources view (Compaction column), Doctor dialog |
| `POST /druid/indexer/v1/compaction/config/datasources/{datasource}`       | Saving the auto-compaction config of a datasource            | Datasources view (Compaction dialog) |
| `DELETE /druid/indexer/v1/compaction/config/datasources/{datasource}`     | Deleting the auto-compaction config of a datasource          | Datasources view (Compaction dialog) |
| `GET /druid/indexer/v1/compaction/config/datasources/{datasource}/history?count=20` | Auto-compaction config change history              | Datasources view (Compaction dialog) |
| `GET /druid/indexer/v1/compaction/status/datasources`                     | Auto-compaction progress ("% Compacted", "Left to be compacted") | Datasources view |
| `POST /druid/coordinator/v1/compaction/compact`                           | Forcing a compaction run                                     | Datasources view |
| `GET /druid/indexer/v1/compaction/config/cluster`                         | Cluster compaction dynamic config                            | Header bar (Compaction dynamic config dialog) |
| `POST /druid/indexer/v1/compaction/config/cluster`                        | Saving the cluster compaction dynamic config                 | Header bar (Compaction dynamic config dialog) |

#### Ingestion: tasks and supervisors

| API                                                                       | Used for                                                     | Where |
|---------------------------------------------------------------------------|--------------------------------------------------------------|-------|
| `POST /druid/indexer/v1/sampler?for={step}`                               | Sampling data for each step of the data loader               | Load data view, Doctor dialog |
| `POST /druid/indexer/v1/task`                                             | Submitting a task                                            | Load data view, Tasks view |
| `GET /druid/indexer/v1/task/{taskId}`                                     | Task payload                                                 | Tasks view (task dialog), Load data view (editing an existing task's spec) |
| `GET /druid/indexer/v1/task/{taskId}/status`                              | Task status                                                  | Tasks view (task dialog) |
| `GET /druid/indexer/v1/task/{taskId}/reports`                             | Task reports                                                 | Tasks view (task dialog) |
| `GET /druid/indexer/v1/task/{taskId}/log`                                 | Task log (tailed)                                            | Tasks view (task dialog) |
| `POST /druid/indexer/v1/task/{taskId}/shutdown`                           | Killing a task                                               | Tasks view |
| `GET /druid/indexer/v1/supervisor`                                        | Listing supervisor IDs                                       | Query view (Supervisor to SQL dialog) |
| `POST /druid/indexer/v1/supervisor`                                       | Submitting (creating or updating) a supervisor               | Load data view, Supervisors view |
| `GET /druid/indexer/v1/supervisor/{supervisorId}`                         | Supervisor spec                                              | Supervisors view (supervisor dialog), Load data view (editing an existing supervisor), Query view (Supervisor to SQL dialog) |
| `GET /druid/indexer/v1/supervisor/{supervisorId}/status`                  | Supervisor status (running tasks, details, recent errors, partition offsets) | Supervisors view (table, supervisor dialog, Reset offsets dialog) |
| `GET /druid/indexer/v1/supervisor/{supervisorId}/stats`                   | Supervisor ingestion stats                                   | Supervisors view (Stats column, supervisor dialog) |
| `GET /druid/indexer/v1/supervisor/{supervisorId}/history?count=100`       | Supervisor spec history                                      | Supervisors view (supervisor dialog) |
| `POST /druid/indexer/v1/supervisor/{supervisorId}/autoscaler/simulate`    | Simulating the auto-scaler                                   | Supervisors view (supervisor dialog) |
| `POST /druid/indexer/v1/supervisor/{supervisorId}/resume`                 | Resuming a supervisor                                        | Supervisors view |
| `POST /druid/indexer/v1/supervisor/{supervisorId}/suspend`                | Suspending a supervisor                                      | Supervisors view |
| `POST /druid/indexer/v1/supervisor/{supervisorId}/reset`                  | Hard resetting a supervisor                                  | Supervisors view |
| `POST /druid/indexer/v1/supervisor/{supervisorId}/resetOffsets`           | Resetting specific partition offsets                         | Supervisors view (Reset offsets dialog) |
| `POST /druid/indexer/v1/supervisor/{supervisorId}/resetToLatestAndBackfill` | Resetting to latest offsets and backfilling                | Supervisors view |
| `POST /druid/indexer/v1/supervisor/{supervisorId}/taskGroups/handoff`     | Triggering early handoff of task groups                      | Supervisors view |
| `POST /druid/indexer/v1/supervisor/{supervisorId}/terminate`              | Terminating a supervisor                                     | Supervisors view |
| `POST /druid/indexer/v1/supervisor/resumeAll`                             | Resuming all supervisors                                     | Supervisors view |
| `POST /druid/indexer/v1/supervisor/suspendAll`                            | Suspending all supervisors                                   | Supervisors view |
| `POST /druid/indexer/v1/supervisor/terminateAll`                          | Terminating all supervisors                                  | Supervisors view |

#### Services and dynamic configs

| API                                                         | Used for                                                     | Where |
|-------------------------------------------------------------|--------------------------------------------------------------|-------|
| `GET /druid/coordinator/v1/loadqueue?simple`                | Segment load/drop queue per Historical                       | Services view (Detail column) |
| `GET /druid/coordinator/v1/config/cloneStatus`              | Historical cloning status                                    | Services view (Detail column) |
| `GET /druid/coordinator/v1/config`                          | Coordinator dynamic config (turbo loading and decommissioning nodes) | Services view (Detail column), Header bar (Coordinator dynamic config dialog) |
| `POST /druid/coordinator/v1/config`                         | Saving the Coordinator dynamic config                        | Header bar (Coordinator dynamic config dialog) |
| `GET /druid/coordinator/v1/config/history?count=100`        | Coordinator dynamic config history                           | Header bar (Coordinator dynamic config dialog) |
| `GET /druid/coordinator/v1/broker/config`                   | Broker dynamic config                                        | Header bar (Broker dynamic config dialog) |
| `POST /druid/coordinator/v1/broker/config`                  | Saving the Broker dynamic config                             | Header bar (Broker dynamic config dialog) |
| `GET /druid/coordinator/v1/broker/config/history?count=100` | Broker dynamic config history                                | Header bar (Broker dynamic config dialog) |
| `GET /druid/indexer/v1/workers`                             | Middle Manager / Indexer worker info (capacity, enabled state) | Services view |
| `POST /druid/indexer/v1/worker/{host}/enable`               | Enabling a Middle Manager                                    | Services view |
| `POST /druid/indexer/v1/worker/{host}/disable`              | Disabling a Middle Manager                                   | Services view |
| `GET /druid/indexer/v1/worker`                              | Overlord dynamic config                                      | Header bar (Overlord dynamic config dialog) |
| `POST /druid/indexer/v1/worker`                             | Saving the Overlord dynamic config                           | Header bar (Overlord dynamic config dialog) |
| `GET /druid/indexer/v1/worker/history?count=100`            | Overlord dynamic config history                              | Header bar (Overlord dynamic config dialog) |

#### Lookups

| API                                                            | Used for                                   | Where |
|----------------------------------------------------------------|--------------------------------------------|-------|
| `GET /druid/coordinator/v1/lookups/config?discover=true`       | Discovering lookup tiers                   | Lookups view |
| `GET /druid/coordinator/v1/lookups/config/all`                 | All lookup specs                           | Lookups view |
| `GET /druid/coordinator/v1/lookups/status`                     | Checking whether lookups are initialized   | Home (Lookups card) |
| `POST /druid/coordinator/v1/lookups/config`                    | Initializing lookups (empty body) and creating a lookup | Lookups view |
| `POST /druid/coordinator/v1/lookups/config/{tier}/{lookupId}`  | Updating a lookup                          | Lookups view |
| `DELETE /druid/coordinator/v1/lookups/config/{tier}/{lookupId}` | Deleting a lookup                         | Lookups view |

### Fallback APIs used when SQL is not available

These APIs are only called when the console detects that the SQL endpoint is not available (for example, when it is
served directly from the Coordinator or Overlord rather than the Router). They stand in for queries against the
`sys.*` and `INFORMATION_SCHEMA` tables and generally provide less information.

| API                                                                                      | Replaces                    | Used for                                                    | Where |
|------------------------------------------------------------------------------------------|-----------------------------|-------------------------------------------------------------|-------|
| `GET /druid/coordinator/v1/datasources`                                                  | `INFORMATION_SCHEMA.TABLES` | Listing datasources                                         | Home (Datasources card), Segment timeline (Datasources and Segments views) |
| `GET /druid/coordinator/v1/datasources?simple`                                           | `sys.segments`              | Datasource list with segment counts and sizes; total available segments | Datasources view, Home (Segments card) |
| `GET /druid/coordinator/v1/loadstatus?simple`                                            | `sys.segments`              | Number of segments left to load per datasource              | Datasources view, Home (Segments card) |
| `GET /druid/coordinator/v1/metadata/datasources`                                         | `sys.segments`              | Resolving the datasource filter                             | Segments view |
| `GET /druid/coordinator/v1/metadata/segments?includeOvershadowedStatus&includeRealtimeSegments` | `sys.segments`       | Segment list; segment intervals for the timeline            | Segments view, Segment timeline (Datasources and Segments views) |
| `GET /druid/coordinator/v1/servers?simple`                                               | `sys.servers`               | Service list; Historical tiers; Peon count                  | Services view, Datasources view (Retention dialog), Home (Services card) |
| `GET /druid/coordinator/v1/cluster`                                                      | `sys.servers`               | Service counts by role                                      | Home (Services card) |
| `GET /druid/indexer/v1/supervisor?full`                                                  | `sys.supervisors`           | Supervisor list                                             | Supervisors view, Home (Supervisors card) |
| `GET /druid/indexer/v1/tasks`                                                            | `sys.tasks`                 | Task list; task counts by status                            | Tasks view, Home (Tasks card) |
| `GET /druid/indexer/v1/tasks?state=running`                                              | `sys.tasks`                 | Running task counts per datasource                          | Datasources view (Running tasks column) |
