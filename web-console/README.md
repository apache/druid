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

This is the Druid web console that servers as a data management interface for Druid.

## Developing the console

### Getting started

1. You need to be within the `web-console` directory
2. Install the modules with `npm install`
3. Run `npm run compile` to compile the scss files (this usually needs to be done only once)
4. Run `npm start` will start in development mode and will proxy druid requests to `localhost:8888`

**Note:** you can provide an environment variable to proxy to a different Druid host like so: `druid_host=1.2.3.4:8888 npm start`
**Note:** you can provide an environment variable use webpack-bundle-analyzer as a plugin in the build script or like so: `BUNDLE_ANALYZER_PLUGIN='TRUE' npm start`

To try the console in (say) coordinator mode you could run it as such:

`druid_host=localhost:8081 npm start`

### Developing

You should use a TypeScript friendly IDE (such as [WebStorm](https://www.jetbrains.com/webstorm/), or [VS Code](https://code.visualstudio.com/)) to develop the web console.

The console relies on [eslint](https://eslint.org) (and various plugins), [sass-lint](https://github.com/sasstools/sass-lint), and [prettier](https://prettier.io/) to enforce code style. If you are going to do any non-trivial development you should set up your IDE to automatically lint and fix your code as you make changes.

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
- Open User Settings (JSON) and set the following:
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

If you change the dependencies of the console in any way please run `script/licenses` (from the web-console directory).
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
`load-data-from-local-disk-error-screenshot.png`.

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
- `e2e-tests/` - End-to-end tests for the console
- `lib/` - A place where keywords and generated docs live.
- `public/` - The compiled destination for the files powering this console
- `script/` - Some helper bash scripts for running this console
- `src/` - This directory (together with `lib`) constitutes all the source code for this console

## List of non SQL data reading APIs used

```
GET /status
GET /druid/indexer/v1/supervisor?full
POST /druid/indexer/v1/worker
GET /druid/indexer/v1/workers
GET /druid/indexer/v1/tasks
GET /druid/coordinator/v1/loadqueue?simple
GET /druid/coordinator/v1/config
GET /druid/coordinator/v1/metadata/datasources?includeUnused
GET /druid/coordinator/v1/rules
GET /druid/coordinator/v1/config/compaction
GET /druid/coordinator/v1/tiers
```
