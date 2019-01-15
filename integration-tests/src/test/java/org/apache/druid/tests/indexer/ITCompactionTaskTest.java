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

package org.apache.druid.tests.indexer;

import com.google.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.RetryUtil;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITCompactionTaskTest extends AbstractIndexerTest
{
  private static final Logger LOG = new Logger(ITCompactionTaskTest.class);
  private static String INDEX_TASK = "/indexer/wikipedia_index_task.json";
  private static String INDEX_QUERIES_RESOURCE = "/indexer/wikipedia_index_queries.json";
  private static String INDEX_DATASOURCE = "wikipedia_index_test";
  private static String COMPACTION_TASK = "/indexer/wikipedia_compaction_task.json";

  @Inject
  private IntegrationTestingConfig config;

  private String fullDatasourceName;

  @BeforeSuite
  public void setFullDatasourceName()
  {
    fullDatasourceName = INDEX_DATASOURCE + config.getExtraDatasourceNameSuffix();
  }

  @Test
  public void testCompactionWithoutKeepSegmentGranularity() throws Exception
  {
    loadData();
    final List<String> intervalsBeforeCompaction = coordinator.getSegmentIntervals(fullDatasourceName);
    intervalsBeforeCompaction.sort(null);
    final String compactedInterval = "2013-08-31T00:00:00.000Z/2013-09-02T00:00:00.000Z";
    if (intervalsBeforeCompaction.contains(compactedInterval)) {
      throw new ISE("Containing a segment for the compacted interval[%s] before compaction", compactedInterval);
    }
    try (final Closeable closeable = unloader(fullDatasourceName)) {
      String queryResponseTemplate;
      try {
        InputStream is = AbstractITBatchIndexTest.class.getResourceAsStream(INDEX_QUERIES_RESOURCE);
        queryResponseTemplate = IOUtils.toString(is, "UTF-8");
      }
      catch (IOException e) {
        throw new ISE(e, "could not read query file: %s", INDEX_QUERIES_RESOURCE);
      }

      queryResponseTemplate = StringUtils.replace(
          queryResponseTemplate,
          "%%DATASOURCE%%",
          fullDatasourceName
      );

      queryHelper.testQueriesFromString(queryResponseTemplate, 2);
      compactData(false);

      // 4 segments across 2 days, compacted into 1 new segment (5 total)
      checkCompactionFinished(5);
      queryHelper.testQueriesFromString(queryResponseTemplate, 2);

      intervalsBeforeCompaction.add(compactedInterval);
      intervalsBeforeCompaction.sort(null);
      checkCompactionIntervals(intervalsBeforeCompaction);
    }
  }

  @Test
  public void testCompactionWithKeepSegmentGranularity() throws Exception
  {
    loadData();
    final List<String> intervalsBeforeCompaction = coordinator.getSegmentIntervals(fullDatasourceName);
    intervalsBeforeCompaction.sort(null);
    try (final Closeable closeable = unloader(fullDatasourceName)) {
      String queryResponseTemplate;
      try {
        InputStream is = AbstractITBatchIndexTest.class.getResourceAsStream(INDEX_QUERIES_RESOURCE);
        queryResponseTemplate = IOUtils.toString(is, "UTF-8");
      }
      catch (IOException e) {
        throw new ISE(e, "could not read query file: %s", INDEX_QUERIES_RESOURCE);
      }

      queryResponseTemplate = StringUtils.replace(
          queryResponseTemplate,
          "%%DATASOURCE%%",
          fullDatasourceName
      );


      queryHelper.testQueriesFromString(queryResponseTemplate, 2);
      compactData(true);

      // 4 segments across 2 days, compacted into 2 new segments (6 total)
      checkCompactionFinished(6);
      queryHelper.testQueriesFromString(queryResponseTemplate, 2);

      checkCompactionIntervals(intervalsBeforeCompaction);
    }
  }

  private void loadData() throws Exception
  {
    String taskSpec = getTaskAsString(INDEX_TASK);
    taskSpec = StringUtils.replace(taskSpec, "%%DATASOURCE%%", fullDatasourceName);
    final String taskID = indexer.submitTask(taskSpec);
    LOG.info("TaskID for loading index task %s", taskID);
    indexer.waitUntilTaskCompletes(taskID);

    RetryUtil.retryUntilTrue(
        () -> coordinator.areSegmentsLoaded(fullDatasourceName),
        "Segment Load"
    );
  }

  private void compactData(boolean keepSegmentGranularity) throws Exception
  {
    final String template = getTaskAsString(COMPACTION_TASK);
    String taskSpec =
        StringUtils.replace(template, "${KEEP_SEGMENT_GRANULARITY}", Boolean.toString(keepSegmentGranularity));
    taskSpec = StringUtils.replace(taskSpec, "%%DATASOURCE%%", fullDatasourceName);

    final String taskID = indexer.submitTask(taskSpec);
    LOG.info("TaskID for compaction task %s", taskID);
    indexer.waitUntilTaskCompletes(taskID);

    RetryUtil.retryUntilTrue(
        () -> coordinator.areSegmentsLoaded(fullDatasourceName),
        "Segment Compaction"
    );
  }

  private void checkCompactionFinished(int numExpectedSegments)
  {
    RetryUtil.retryUntilTrue(
        () -> {
          int metadataSegmentCount = coordinator.getMetadataSegments(fullDatasourceName).size();
          LOG.info("Current metadata segment count: %d, expected: %d", metadataSegmentCount, numExpectedSegments);
          return metadataSegmentCount == numExpectedSegments;
        },
        "Compaction segment count check"
    );
  }

  private void checkCompactionIntervals(List<String> expectedIntervals)
  {
    RetryUtil.retryUntilTrue(
        () -> {
          final List<String> intervalsAfterCompaction = coordinator.getSegmentIntervals(fullDatasourceName);
          intervalsAfterCompaction.sort(null);
          System.out.println("AFTER: " + intervalsAfterCompaction);
          System.out.println("EXPECTED: " + expectedIntervals);
          return intervalsAfterCompaction.equals(expectedIntervals);
        },
        "Compaction interval check"
    );
  }
}
