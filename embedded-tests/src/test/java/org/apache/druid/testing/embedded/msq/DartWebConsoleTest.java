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

package org.apache.druid.testing.embedded.msq;

import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.console.WebConsoleTestBase;
import org.junit.jupiter.api.Test;

/**
 * Runs and cancels Dart queries from the Query view of the web console, following them in its current Dart queries
 * panel, as {@link EmbeddedDartReportApiTest} does through the API.
 */
public class DartWebConsoleTest extends WebConsoleTestBase
{
  @Override
  protected void configureCluster(EmbeddedDruidCluster cluster)
  {
    // Dart runs the controller of a query on the Broker and its workers on the Historicals, with a part of their heap
    // (as in EmbeddedDartReportApiTest), which needs to be more than the 100 MB that embedded servers have by default
    broker.setServerMemory(1_000_000_000L)
          .addProperty("druid.msq.dart.controller.heapFraction", "0.5");
    historical.setServerMemory(1_000_000_000L)
              .addProperty("druid.msq.dart.worker.heapFraction", "0.5")
              .addProperty("druid.msq.dart.worker.concurrentQueries", "1");
  }

  @Test
  public void testDart() throws Exception
  {
    runSpec("dart.spec.ts");
  }
}
