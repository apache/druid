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

import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;

/**
 * Base class for embedded tests related to ingestion and indexing.
 * This class should not contain any hidden configs or setup, only shorthand
 * utility methods.
 * <p>
 * Steps:
 * <ul>
 * <li>Write a {@code *Test} class that extends this class.</li>
 * <li>Create an {@link EmbeddedDruidCluster} containing all servers, resources,
 * extensions and properties in {@link #createCluster()}.</li>
 * <li>Write one or more {@code @Test} (JUnit5) methods.</li>
 * </ul>
 * Base class for JUnit 5 tests that use an {@link EmbeddedDruidCluster}.
 * This base class is responsible for setting up the cluster before <b>ANY<b/>
 * test method has run and tearing it down after <b>ALL</b> the tests have run.
 * <p>
 * Usage:
 * <pre>
 * public class IndexingTaskTest implements DruidSimulationTestBase
 * {
 *    private final EmbeddedOverlord overlord = new EmbeddedOverlord();
 *
 *    &#64;Override
 *    public EmbeddedDruidCluster buildCluster()
 *    {
 *      return EmbeddedDruidCluster.withExtensions(List.of())
 *                                 .addServer(overlord)
 *                                 .addServer(new EmbeddedCoordinator())
 *                                 .addServer(new EmbeddedIndexer())
 *                                 .build();
 *    }
 *
 *    &#64;Test
 *    public void test_runIndexTask()
 *    {
 *      final String taskId = IdUtils.newTaskId();
 *      getResult(cluster.leaderOverlord().runTask(taskId, task));
 *
 *      cluster.overlord().waitUntilTaskFinishes(taskId);
 *
 *      Assertions.assertEquals(
 *          TaskState.SUCCESS,
 *          getResult(cluster.leaderOverlord().taskStatus(taskId)).getState().getCode()
 *      );
 *    }
 * }
 * </pre>
 *
 * @see EmbeddedDruidCluster for information on how to build a cluster
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DruidEmbeddedTestBase
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

  @BeforeEach
  protected void beforeEachTest()
  {
    dataSource = EmbeddedClusterApis.createTestDatasourceName();
  }
}
