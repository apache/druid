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

package org.apache.druid.testing.embedded.junit5;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.DruidNode;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedDruidServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;

/**
 * Base class for Junit5 tests that use an {@link EmbeddedDruidCluster}.
 * <p>
 * Steps to write a test:
 * <ul>
 * <li>Write an {@code Embedded*Test} class that extends this class.</li>
 * <li>Create an {@link EmbeddedDruidCluster} containing all servers, resources,
 * extensions and properties in {@link #createCluster()}.</li>
 * <li>Write one or more {@code @Test} (JUnit5) methods.</li>
 * </ul>
 * The cluster is created before <b>ANY<b/> test method has run and is torn down
 * after <b>ALL</b> the tests have run.
 * <p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class EmbeddedClusterTestBase
{
  /**
   * Cluster used in this test, created in {@link #createCluster()}.
   */
  protected EmbeddedDruidCluster cluster;

  /**
   * Random test datasource name that is freshly generated for each test method.
   */
  protected String dataSource;

  /**
   * Returns the URL of the requested server.
   */
  protected static String getServerUrl(EmbeddedDruidServer<?> server)
  {
    final DruidNode node = server.bindings().selfNode();
    return StringUtils.format(
        "http://%s:%s",
        node.getHost(),
        node.getPlaintextPort()
    );
  }

  /**
   * Returns the TLS-enabled url of the given server.
   */
  protected static String getServerTlsUrl(EmbeddedDruidServer<?> server)
  {
    final DruidNode node = server.bindings().selfNode();
    return StringUtils.format(
        "http://%s:%s",
        node.getHost(),
        node.getTlsPort()
    );
  }

  /**
   * Creates the cluster to be used in this test class. This method is invoked
   * only once before any of the {@code @Test} methods have run.
   * Implementations of this method should not start the cluster as it is done in
   * {@link #setup()}.
   */
  protected abstract EmbeddedDruidCluster createCluster();

  @BeforeAll
  protected void setup() throws Exception
  {
    cluster = createCluster();
    cluster.start();
  }

  @AfterAll
  protected void tearDown()
  {
    if (cluster != null) {
      cluster.stop();
    }
  }

  /**
   * Assigns a new value to the {@link #dataSource} before each test.
   */
  @BeforeEach
  protected void refreshDatasourceName()
  {
    dataSource = EmbeddedClusterApis.createTestDatasourceName();
  }
}
