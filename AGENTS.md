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


# Apache Druid

Real-time analytics database. Java, Maven, multi-module project.

## Key Modules

- `processing/`: Core query processing, aggregation, data structures
- `server/`: Common server functionality, HTTP services
- `sql/`: SQL planning (Calcite), SQL-to-native translation
- `indexing-service/`: Task framework, ingestion coordination
- `multi-stage-query/`: MSQ engine
- `extensions-core/`: Core extensions (S3, Kafka, Kinesis, Parquet, DataSketches, etc.)
- `web-console/`: React/TypeScript management UI
- `quidem-ut/`: SQL query testing framework

## Code Style

- Read `dev/style-conventions.md` for conventions.
- Forbidden APIs: `codestyle/druid-forbidden-apis.txt`.
- **Always use `final`** for fields and variables that are not reassigned.
- End every file with a newline.
- Don't format changes unnecessarily.

## Testing

### JUnit 5 test helpers

When adding or migrating tests, reuse these existing test-scope helpers:

- `TemporaryFolderExtension` (`org.apache.druid.testing`) for tests that need a Druid-managed `File` temporary directory:

  ```java
  @RegisterExtension
  public final TemporaryFolderExtension temporaryFolder = new TemporaryFolderExtension();
  ```

  Use `getRoot()`, `newFolder(...)`, or `newFile(...)`. Prefer `TemporaryFolderExtension` over JUnit 5 `@TempDir` or JUnit 4 `TemporaryFolder`. Do not add new `TempFolderOperations` usages; the existing compatibility bridge is only for tests that have not yet migrated.

- `LoggerCaptureExtension` (`org.apache.druid.testing.junit`) for capturing Log4j events. Register it with the target class and use `getLogEvents()`, `clearLogEvents()`, or `awaitLogEvents()` as needed.

## Pull Requests

- Before opening a PR, self-review the complete diff against the target branch. Verify that every change is intentional and in scope, check for correctness and regressions, and run the relevant tests or checks.
- PR titles targeting `master` must use the Conventional Commits format: `<type>: <description>` or `<type>(<scope>): <description>`.
- For a breaking change, add `!` immediately before the colon: `<type>!: <description>` or `<type>(<scope>)!: <description>`. A breaking change is backward-incompatible and may require users to update existing code, configuration, or integrations.
- Accepted types are `backport`, `build`, `ci`, `dev`, `docs`, `feat`, `fix`, `minor`, `perf`, `refactor`, `release`, `revert`, `style`, and `test`.
- Example: `build: remove redundant license download step`.
- Follow `.github/pull_request_template.md` when preparing the PR description.

## Running Tests

Use these flags for faster tests: `-Pskip-static-checks -Dweb.console.skip=true -T1C`

**Single test method:**
```
mvn test -pl sql -am -Dtest="org.apache.druid.sql.calcite.CalciteQueryTest#testFoo" -Dsurefire.failIfNoSpecifiedTests=false -Pskip-static-checks -Dweb.console.skip=true -T1C
```

**All tests in a package:**
```
mvn test -pl sql -am -Dtest="org.apache.druid.sql.**" -Dsurefire.failIfNoSpecifiedTests=false -Pskip-static-checks -Dweb.console.skip=true -T1C
```

**Quidem (.iq) tests:**

To run `sql/src/test/quidem/org.apache.druid.quidem.SqlQuidemTest/numMerge.iq`:

```
mvn test -pl sql -am -Dtest="org.apache.druid.sql.calcite.SqlQuidemTest" -Dquidem.filter=numMerge -Dsurefire.failIfNoSpecifiedTests=false -Pskip-static-checks -Dweb.console.skip=true -T1C
```

To run `quidem-ut/src/test/quidem/org.apache.druid.quidem.QTest/qaWin/basics_group_by.all.iq`:

```
mvn test -pl quidem-ut -am -Dtest="org.apache.druid.quidem.QTest" -Dquidem.filter=qaWin/basics_group_by.all -Dsurefire.failIfNoSpecifiedTests=false -Pskip-static-checks -Dweb.console.skip=true -T1C
```

Always use `-Dquidem.filter` to avoid running the full suite. To run multiple tests, use commas like
`-Dquidem.filter=file1,file2,file3` or use wildcards like `-Dquidem.filter=join*`. Include subdirectory names if
present, such as `-Dquidem.filter=qaWin/**`.


### Web Console Development

Refer to `web-console/README.md` for general instructions on developing the web console.

Run `npm run test-unit` from the `web-console/` directory to verify your work. Before doing this for the first time
in a fresh checkout, you will also need to run `npm install`.

Run `npm run autofix` from the `web-console/` directory to fix formatting issues.

### Documentation

- When updating Markdown tables in `docs/`, preserve the existing table formatting.
- Use backquotes for any code references. e.g., `serverPriorityToReplicas`, `ioConfig`.
- Run `npm run spellcheck` from the `website/` directory to verify doc updates and update the `.spelling` file if needed.
