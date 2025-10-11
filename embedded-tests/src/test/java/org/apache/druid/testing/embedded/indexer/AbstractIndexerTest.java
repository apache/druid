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

package org.apache.druid.testing.embedded.indexer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.commons.io.IOUtils;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchModule;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchModule;
import org.apache.druid.query.aggregation.datasketches.theta.SketchModule;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.BeforeAll;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractIndexerTest extends EmbeddedClusterTestBase
{
  protected static class PlaceHolders
  {
    protected static final String DATASOURCE = "%%DATASOURCE%%";
    protected static final String DATA_DIRECTORY = "%%DATA_DIRECTORY%%";
  }

  protected final EmbeddedCoordinator coordinator = new EmbeddedCoordinator()
      .addProperty("druid.manager.segments.useIncrementalCache", "always");
  protected final EmbeddedOverlord overlord = new EmbeddedOverlord();
  protected final EmbeddedIndexer indexer = new EmbeddedIndexer()
      .setServerMemory(500_000_000L)
      .addProperty("druid.worker.capacity", "5")
      .addProperty("druid.segment.handoff.pollDuration", "PT0.1s");
  protected final EmbeddedBroker broker = new EmbeddedBroker();

  /**
   * Remove usages of this mapper and use TaskBuilder instead.
   */
  protected ObjectMapper jsonMapper;
  protected QueryResultsVerifier queryResultsVerifier = null;

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    final EmbeddedDruidCluster cluster = EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper();
    addResources(cluster);

    cluster
        .useLatchableEmitter()
        .addExtensions(SketchModule.class, DoublesSketchModule.class, HllSketchModule.class)
        .addServer(coordinator)
        .addServer(overlord)
        .addServer(indexer)
        .addServer(broker)
        .addServer(new EmbeddedHistorical())
        .addServer(new EmbeddedRouter());

    return cluster;
  }

  protected void addResources(EmbeddedDruidCluster cluster)
  {
    // Used by test classes to add extensions
  }

  @BeforeAll
  public void initJsonMapper()
  {
    this.jsonMapper = overlord.bindings().jsonMapper();
    this.queryResultsVerifier = new QueryResultsVerifier(cluster, jsonMapper);
  }

  protected Closeable unloader(final String dataSource)
  {
    return cluster.callApi().createUnloader(dataSource);
  }

  /**
   * Submits the given task payload to the Overlord.
   * This method will be updated later to use TaskBuilder instead.
   */
  protected String submitTask(String taskSpec)
  {
    final String taskID = IdUtils.getRandomId();
    final Map<String, Object> taskPayload = EmbeddedClusterApis.deserializeJsonToMap(taskSpec);
    taskPayload.put("id", taskID);

    cluster.callApi().onLeaderOverlord(o -> o.runTask(taskID, taskPayload));
    return taskID;
  }

  protected void waitForAllTasksToCompleteForDataSource(final String dataSource)
  {
    // Wait for any existing kill tasks to complete before submitting new index task otherwise
    // kill tasks can fail with interval lock revoked.
    final Set<String> taskIds = getTaskIdsForState(dataSource);
    for (String taskId : taskIds) {
      cluster.callApi().waitForTaskToSucceed(taskId, overlord);
    }
  }

  public static String getResourceAsString(String file) throws IOException
  {
    try (final InputStream inputStream = getResourceAsStream(file)) {
      return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
    }
  }

  public static InputStream getResourceAsStream(String resource)
  {
    return AbstractIndexerTest.class.getResourceAsStream(resource);
  }

  private Set<String> getTaskIdsForState(String dataSource)
  {
    return ImmutableList.copyOf(
        (CloseableIterator<TaskStatusPlus>) cluster.callApi().onLeaderOverlord(o -> o.taskStatuses(null, dataSource, 0))
    ).stream().map(TaskStatusPlus::getId).collect(Collectors.toSet());
  }
}
