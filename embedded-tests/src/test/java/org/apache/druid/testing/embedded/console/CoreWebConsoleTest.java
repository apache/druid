/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.testing.embedded.console;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

/**
 * Runs the web console specs that need only a plain cluster (no external resources).
 */
public class CoreWebConsoleTest extends WebConsoleTestBase
{
  @ParameterizedTest(name = "{0}")
  @ValueSource(strings = {
      "auto-compaction.spec.ts",
      "cancel-query.spec.ts",
      "multi-stage-query.spec.ts",
      "reindexing.spec.ts",
      "tutorial-batch.spec.ts"
  })
  public void testSpec(String spec) throws Exception
  {
    runSpec(spec, Map.of());
  }
}
