/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.tests.indexer;

import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.testing.guice.DruidTestModuleFactory;
import io.druid.testing.utils.RetryUtil;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.List;

@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITCompactionTaskTest extends AbstractIndexerTest
{
  private static final Logger LOG = new Logger(ITCompactionTaskTest.class);
  private static String INDEX_TASK = "/indexer/wikipedia_index_task.json";
  private static String INDEX_QUERIES_RESOURCE = "/indexer/wikipedia_index_queries.json";
  private static String INDEX_DATASOURCE = "wikipedia_index_test";
  private static String COMPACTION_TASK = "/indexer/wikipedia_compaction_task.json";
  private static String COMPACTED_INTERVAL = "2013-08-31T00:00:00.000Z/2013-09-02T00:00:00.000Z";

  @Test
  public void testCompaction() throws Exception
  {
    loadData();
    final List<String> intervalsBeforeCompaction = coordinator.getSegmentIntervals(INDEX_DATASOURCE);
    if (intervalsBeforeCompaction.contains(COMPACTED_INTERVAL)) {
      throw new ISE("Containing a segment for the compacted interval[%s] before compaction", COMPACTED_INTERVAL);
    }
    try {
      queryHelper.testQueriesFromFile(INDEX_QUERIES_RESOURCE, 2);
      compactData();
      queryHelper.testQueriesFromFile(INDEX_QUERIES_RESOURCE, 2);

      final List<String> intervalsAfterCompaction = coordinator.getSegmentIntervals(INDEX_DATASOURCE);
      if (!intervalsAfterCompaction.contains(COMPACTED_INTERVAL)) {
        throw new ISE("Compacted segment for interval[%s] does not exist", COMPACTED_INTERVAL);
      }
    }
    finally {
      unloadAndKillData(INDEX_DATASOURCE);
    }
  }

  private void loadData() throws Exception
  {
    final String taskID = indexer.submitTask(getTaskAsString(INDEX_TASK));
    LOG.info("TaskID for loading index task %s", taskID);
    indexer.waitUntilTaskCompletes(taskID);

    RetryUtil.retryUntilTrue(
        () -> coordinator.areSegmentsLoaded(INDEX_DATASOURCE),
        "Segment Load"
    );
  }

  private void compactData() throws Exception
  {
    final String taskID = indexer.submitTask(getTaskAsString(COMPACTION_TASK));
    LOG.info("TaskID for compaction task %s", taskID);
    indexer.waitUntilTaskCompletes(taskID);

    RetryUtil.retryUntilTrue(
        () -> coordinator.areSegmentsLoaded(INDEX_DATASOURCE),
        "Segment Compaction"
    );
  }
}
