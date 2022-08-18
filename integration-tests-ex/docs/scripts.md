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

# Scripts

The IT framework uses a number of scripts and it can be a bit of a mystery
what each one does. This guide lists each of them.

## `integration-tests-ex`

* `it.sh` - Utility to perform many IT-related actions such as building Druid,
  running ITs, starting a cluster, etc. Use `it.sh help` to see the list of commands.

### `it-image`

* `build-image.sh` - Internal script to (you guessed it), build the image.
  Creates the `target/env.sh` file above, then invokes `docker-build.sh`.
* `rebuild.sh` - Rebuilds the image after the above script has created the
  `env.sh` file. Used to debug changes to the image build itself: use `rebuild.sh`
  rather than waiting for Maven to do its thing.
* `docker-build.sh` - Internal script to set up the needed environment
  variables and invoke Docker to build the image. Modify this if you ned to change
  the information passed into Docker via the command line.
* `docker/Dockerfile` - Docker script to build the image.
* `docker/test-setup.sh` - Script copied into the image at build time and run to
  set up the image. Keeps the `Dockerfile` simpler (and, it is somewhat easier to
  debug this script than the `Dockerfile` itself.)
* `docker/launch.sh` - Container entrypoint which runs inside the container.
  Sets up configuration and calls `druid.sh` to launch Druid itself.
* `druid.sh` - Creates a Druid configuration file from environment variables,
  then runs Druid within the container.
* `target/env.sh` - Created when the image is built. Provides environment
  variables for things like the image name, versions and so on. Used to
  quickly rebuild the image (see [Maven configuration](docs/maven.md)) and
  to launch tests.

### `test-cases`

* `cluster/<category>/*.yaml` - Base Docker Compose scripts that define the "standard"
  Druid cluster. Tests use these files to avoid redundant copy/past of the
  standard items.
* `cluster.sh` - Launches or tears down a cluster for a test. Called from Maven
  and `it.sh`. Can be used manually. See below.

The options for `cluster.sh` are:

```bash
cluster.sh [-h|help|up|down|status|compose-cmd] category
```

* `up` - starts the cluster.
* `down` - shuts down the cluster.
* `status` - displays cluster status for debugging. Expecially useful for debugging
  issues in Travis where we cannot directly inspect the Docker cluster itself.
* `help` - repeats the usage line.
* Others - passes the command on to Docker Compose.
* `category` - the test category to launch. Performs mapping from the category name
  to cluster name when categories share definitions.
