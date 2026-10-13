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

# 🔁 Stability Runner

Runs chosen tests 64× (8 parallel shards × 8 sequential runs) to reproduce a flaky test or prove a fix.
No surefire reruns, failures don't stop the loop → every failure counts.

## ⚙️ `STABILITY_MAVEN_ARGS`

```
STABILITY_MAVEN_ARGS=-pl embedded-tests -Dtest=KafkaIndexFaultToleranceTest
```

* One line; last one wins.
* Split on whitespace — quotes are not interpreted.
* Used for `install -am -DskipTests`, then for each `test` run.
* Preset: `-Pci,skip-static-checks -Dweb.console.skip=true -Dsurefire.failIfNoSpecifiedTests=false`.

## 🚀 Triggers

* 💬 **PR review** — committer (`MEMBER`/`OWNER`) submits a review containing the line.
  Same args again → previous run cancelled; different args → run side by side. Executes the PR's code.
* 🌿 **Push** — branch `stability/**`; args from newest of last 20 commit messages carrying the line.
* 🖐️ **Manual** — `gh workflow run stability.yml --ref <branch> -f maven_args='<args>' [-f jdk=25]`.

## 📦 Results

* Checks: `Test Report - stability-[<id>-]shard-<N>`.
* Artifacts: `stability-jdk<jdk>-<hash>`, one per shard →
  `stability-results/run<N>/` with `**/target/surefire-reports` and `task-logs`.
