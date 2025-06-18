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

package org.apache.druid.testing.simulate.junit5;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.testing.simulate.EmbeddedDruidCluster;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * JUnit 5 extension to run unit tests with embedded Druid clusters.
 *
 * @see EmbeddedDruidCluster
 * @see DruidSimulationTestBase
 */
public class EmbeddedDruidClusterExtension implements BeforeAllCallback, AfterAllCallback
{
  private volatile EmbeddedDruidCluster cluster;

  @Override
  public void beforeAll(ExtensionContext extensionContext) throws Exception
  {
    final Object testInstance = extensionContext.getTestInstance().orElseThrow(
        () -> new ISE(
            "Test class must be annotated with '@TestInstance(TestInstance.Lifecycle.PER_CLASS)'"
        )
    );
    if (!(testInstance instanceof DruidSimulationTestBase)) {
      throw new ISE("Test class must implement DruidClusterTest");
    }

    cluster = ((DruidSimulationTestBase) testInstance).createCluster();
    cluster.before();
  }

  @Override
  public void afterAll(ExtensionContext extensionContext)
  {
    if (cluster != null) {
      cluster.after();
    }
  }
}
