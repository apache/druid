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

package org.apache.druid.testing.embedded.compact;

import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;

import java.io.Closeable;
import java.util.List;
import java.util.Set;

public abstract class CompactionTestBase extends EmbeddedClusterTestBase
{
  protected final EmbeddedOverlord overlord = new EmbeddedOverlord();
  protected final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addServer(overlord)
                               .addServer(coordinator)
                               .addServer(new EmbeddedIndexer())
                               .addServer(new EmbeddedBroker())
                               .addServer(new EmbeddedHistorical())
                               .addServer(new EmbeddedRouter());
  }

  /**
   * Deletes all the data for the given datasource so that compaction tasks for
   * this datasource do not take up task slots unnecessarily.
   */
  protected Closeable unloader(String dataSource)
  {
    return cluster.callApi().createUnloader(dataSource);
  }

  /**
   * Creates a Task using the given builder and runs it.
   *
   * @return ID of the task.
   */
  protected String runTask(TaskBuilder<?, ?, ?> taskBuilder, String dataSource)
  {
    return cluster.callApi().runTask(
        (ds, taskId) -> taskBuilder.dataSource(ds).withId(taskId),
        dataSource,
        overlord,
        coordinator
    );
  }

  protected void verifySegmentIntervals(List<Interval> expectedIntervals)
  {
    Assertions.assertEquals(
        Set.copyOf(expectedIntervals),
        Set.copyOf(getSegmentIntervals(dataSource))
    );
  }

  protected List<Interval> getSegmentIntervals(String dataSource)
  {
    return cluster.callApi().getSortedSegmentIntervals(dataSource, overlord);
  }

  protected void verifySegmentsCount(int numExpectedSegments)
  {
    cluster.callApi().verifyNumVisibleSegmentsIs(numExpectedSegments, dataSource, overlord);
  }

  protected void verifyQuery(List<Pair<String, String>> queries)
  {
    if (queries == null) {
      return;
    }
    queries.forEach(
        pair -> cluster.callApi().verifySqlQuery(pair.lhs, dataSource, pair.rhs)
    );
  }
}
