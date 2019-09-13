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
import org.apache.druid.indexing.common.task.batch.parallel.PartialSegmentGenerateTask;
import org.apache.druid.indexing.common.task.batch.parallel.PartialSegmentMergeTask;
import org.apache.druid.indexing.common.task.batch.parallel.SinglePhaseSubTask;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.clients.ClientInfoResourceTestClient;
import org.apache.druid.testing.utils.RetryUtil;
import org.apache.druid.testing.utils.SqlTestQueryHelper;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.testng.Assert;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Function;

public abstract class AbstractITBatchIndexTest extends AbstractIndexerTest
{
  private static final Logger LOG = new Logger(AbstractITBatchIndexTest.class);

  @Inject
  IntegrationTestingConfig config;
  @Inject
  protected SqlTestQueryHelper sqlQueryHelper;

  @Inject
  ClientInfoResourceTestClient clientInfoResourceTestClient;

  void doIndexTest(
      String dataSource,
      String indexTaskFilePath,
      String queryFilePath,
      boolean waitForNewVersion
  ) throws IOException
  {
    doIndexTest(dataSource, indexTaskFilePath, Function.identity(), queryFilePath, waitForNewVersion);
  }

  void doIndexTest(
      String dataSource,
      String indexTaskFilePath,
      Function<String, String> taskSpecTransform,
      String queryFilePath,
      boolean waitForNewVersion
  ) throws IOException
  {
    final String fullDatasourceName = dataSource + config.getExtraDatasourceNameSuffix();
    final String taskSpec = taskSpecTransform.apply(
        StringUtils.replace(
            getResourceAsString(indexTaskFilePath),
            "%%DATASOURCE%%",
            fullDatasourceName
        )
    );

    submitTaskAndWait(taskSpec, fullDatasourceName, waitForNewVersion);
    try {

      String queryResponseTemplate;
      try {
        InputStream is = AbstractITBatchIndexTest.class.getResourceAsStream(queryFilePath);
        queryResponseTemplate = IOUtils.toString(is, StandardCharsets.UTF_8);
      }
      catch (IOException e) {
        throw new ISE(e, "could not read query file: %s", queryFilePath);
      }

      queryResponseTemplate = StringUtils.replace(
          queryResponseTemplate,
          "%%DATASOURCE%%",
          fullDatasourceName
      );
      queryHelper.testQueriesFromString(queryResponseTemplate, 2);

    }
    catch (Exception e) {
      LOG.error(e, "Error while testing");
      throw new RuntimeException(e);
    }
  }

  void doReindexTest(
      String baseDataSource,
      String reindexDataSource,
      String reindexTaskFilePath,
      String queryFilePath
  ) throws IOException
  {
    doReindexTest(baseDataSource, reindexDataSource, Function.identity(), reindexTaskFilePath, queryFilePath);
  }

  void doReindexTest(
      String baseDataSource,
      String reindexDataSource,
      Function<String, String> taskSpecTransform,
      String reindexTaskFilePath,
      String queryFilePath
  ) throws IOException
  {
    final String fullBaseDatasourceName = baseDataSource + config.getExtraDatasourceNameSuffix();
    final String fullReindexDatasourceName = reindexDataSource + config.getExtraDatasourceNameSuffix();

    String taskSpec = StringUtils.replace(
        getResourceAsString(reindexTaskFilePath),
        "%%DATASOURCE%%",
        fullBaseDatasourceName
    );

    taskSpec = StringUtils.replace(
        taskSpec,
        "%%REINDEX_DATASOURCE%%",
        fullReindexDatasourceName
    );

    taskSpec = taskSpecTransform.apply(taskSpec);

    submitTaskAndWait(taskSpec, fullReindexDatasourceName, false);
    try {
      String queryResponseTemplate;
      try {
        InputStream is = AbstractITBatchIndexTest.class.getResourceAsStream(queryFilePath);
        queryResponseTemplate = IOUtils.toString(is, StandardCharsets.UTF_8);
      }
      catch (IOException e) {
        throw new ISE(e, "could not read query file: %s", queryFilePath);
      }

      queryResponseTemplate = StringUtils.replace(
          queryResponseTemplate,
          "%%DATASOURCE%%",
          fullReindexDatasourceName
      );

      queryHelper.testQueriesFromString(queryResponseTemplate, 2);
      // verify excluded dimension is not reIndexed
      final List<String> dimensions = clientInfoResourceTestClient.getDimensions(
          fullReindexDatasourceName,
          "2013-08-31T00:00:00.000Z/2013-09-10T00:00:00.000Z"
      );
      Assert.assertFalse(dimensions.contains("robot"), "dimensions : " + dimensions);
    }
    catch (Exception e) {
      LOG.error(e, "Error while testing");
      throw new RuntimeException(e);
    }
  }

  void doIndexTestSqlTest(
      String dataSource,
      String indexTaskFilePath,
      String queryFilePath
  ) throws IOException
  {
    final String fullDatasourceName = dataSource + config.getExtraDatasourceNameSuffix();
    final String taskSpec = StringUtils.replace(
        getResourceAsString(indexTaskFilePath),
        "%%DATASOURCE%%",
        fullDatasourceName
    );

    submitTaskAndWait(taskSpec, fullDatasourceName, false);
    try {
      sqlQueryHelper.testQueriesFromFile(queryFilePath, 2);
    }
    catch (Exception e) {
      LOG.error(e, "Error while testing");
      throw new RuntimeException(e);
    }
  }

  private void submitTaskAndWait(String taskSpec, String dataSourceName, boolean waitForNewVersion)
  {
    final List<DataSegment> oldVersions = waitForNewVersion ? coordinator.getAvailableSegments(dataSourceName) : null;

    long startSubTaskCount = -1;
    final boolean assertRunsSubTasks = taskSpec.contains("index_parallel");
    if (assertRunsSubTasks) {
      startSubTaskCount = countCompleteSubTasks(dataSourceName, !taskSpec.contains("dynamic"));
    }

    final String taskID = indexer.submitTask(taskSpec);
    LOG.info("TaskID for loading index task %s", taskID);
    indexer.waitUntilTaskCompletes(taskID);

    if (assertRunsSubTasks) {
      final boolean perfectRollup = !taskSpec.contains("dynamic");
      final long newSubTasks = countCompleteSubTasks(dataSourceName, perfectRollup) - startSubTaskCount;
      Assert.assertTrue(
          newSubTasks > 0,
          StringUtils.format(
              "The supervisor task[%s] didn't create any sub tasks. Was it executed in the parallel mode?",
              taskID
          )
      );
    }

    // ITParallelIndexTest does a second round of ingestion to replace segements in an existing
    // data source. For that second round we need to make sure the coordinator actually learned
    // about the new segments befor waiting for it to report that all segments are loaded; otherwise
    // this method could return too early because the coordinator is merely reporting that all the
    // original segments have loaded.
    if (waitForNewVersion) {
      RetryUtil.retryUntilTrue(
          () -> {
            final VersionedIntervalTimeline<String, DataSegment> timeline = VersionedIntervalTimeline.forSegments(
                coordinator.getAvailableSegments(dataSourceName)
            );

            final List<TimelineObjectHolder<String, DataSegment>> holders = timeline.lookup(Intervals.ETERNITY);
            return holders
                .stream()
                .flatMap(holder -> holder.getObject().stream())
                .anyMatch(chunk -> oldVersions.stream()
                                              .anyMatch(oldSegment -> chunk.getObject().overshadows(oldSegment)));
          },
          "See a new version"
      );
    }

    RetryUtil.retryUntilTrue(
        () -> coordinator.areSegmentsLoaded(dataSourceName), "Segment Load"
    );
  }

  private long countCompleteSubTasks(final String dataSource, final boolean perfectRollup)
  {
    return indexer.getCompleteTasksForDataSource(dataSource)
                  .stream()
                  .filter(t -> {
                    if (!perfectRollup) {
                      return t.getType().equals(SinglePhaseSubTask.TYPE);
                    } else {
                      return t.getType().equalsIgnoreCase(PartialSegmentGenerateTask.TYPE)
                             || t.getType().equalsIgnoreCase(PartialSegmentMergeTask.TYPE);
                    }
                  })
                  .count();
  }
}
