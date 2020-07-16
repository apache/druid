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

This is the unified Druid web console that servers as a data management layer for Druid.

## How to watch and run for development

1. You need to be withing the `web-console` directory
2. Install the modules with `npm install`
3. Run `npm start` will start in development mode and will proxy druid requests to `localhost:8888`

**Note:** you can provide an environment variable to proxy to a different Druid host like so: `druid_host=1.2.3.4:8888 npm start`
**Note:** you can provide an environment variable use webpack-bundle-analyzer as a plugin in the build script or like so: `BUNDLE_ANALYZER_PLUGIN='TRUE' npm start`

To try the console in (say) coordinator mode you could run it as such:

`druid_host=localhost:8081 npm start`

## Description of the directory structure

A lot of the directory structure was created to preserve the existing console structure as much as possible.

As part of this repo:

- `assets/` - The images (and other assets) used within the console
- `e2e-tests/` - End-to-end tests for the console
- `lib/` - A place where some overrides to the react-table stylus files live, this is outside of the normal SCSS build system.
- `public/` - The compiled destination of the file powering this console
- `script/` - Some helper bash scripts for running this console
- `src/` - This directory (together with `lib`) constitutes all the source code for this console

Generated/copied dynamically

- `index.html` - Entry file for the coordinator console
- `pages/` - The files for the older coordinator console
- `coordinator-console/` - Files for the coordinator console

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

## Updating the list of license files

From the web-console directory run `script/licenses`

## Running End-to-End Tests

From the web-console directory:

1. Build druid distribution: `script/druid build`
2. Start druid cluster: `script/druid start`
3. Run end-to-end tests: `npm run test-e2e`
4. Stop druid cluster: `script/druid stop`

### Debugging

#### Screenshots

`e2e-tests/util/debug.ts:saveScreenshotIfError()` is used to save a screenshot of the web console
when the test fails. For example, if `e2e-tests/tutotrial-batch.spec.ts` fails, it will create
`load-data-from-local-disk-error-screenshot.png`.

#### Disabling Headless Mode

Disabling headless mode while running the tests can be helpful. One way of doing this is by using
`e2e-tests/util/playwright:createBrowserDebug()`. For example, the test can be modified to change

```
import { createBrowserNormal as createBrowser } from './util/playwright'
```

to

```
import { createBrowserDebug as createBrowser } from './util/playwright'`
```

