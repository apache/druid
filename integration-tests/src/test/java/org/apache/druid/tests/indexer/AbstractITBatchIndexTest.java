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

import com.google.common.collect.FluentIterable;
import com.google.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.apache.druid.indexer.partitions.SecondaryPartitionType;
import org.apache.druid.indexing.common.task.batch.parallel.PartialDimensionCardinalityTask;
import org.apache.druid.indexing.common.task.batch.parallel.PartialDimensionDistributionTask;
import org.apache.druid.indexing.common.task.batch.parallel.PartialGenericSegmentMergeTask;
import org.apache.druid.indexing.common.task.batch.parallel.PartialHashSegmentGenerateTask;
import org.apache.druid.indexing.common.task.batch.parallel.PartialRangeSegmentGenerateTask;
import org.apache.druid.indexing.common.task.batch.parallel.SinglePhaseSubTask;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.clients.ClientInfoResourceTestClient;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.testing.utils.SqlTestQueryHelper;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.testng.Assert;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public abstract class AbstractITBatchIndexTest extends AbstractIndexerTest
{
  public enum InputFormatDetails
  {
    AVRO("avro_ocf", ".avro", "/avro"),
    CSV("csv", ".csv", "/csv"),
    TSV("tsv", ".tsv", "/tsv"),
    ORC("orc", ".orc", "/orc"),
    JSON("json", ".json", "/json"),
    PARQUET("parquet", ".parquet", "/parquet");

    private final String inputFormatType;
    private final String fileExtension;
    private final String folderSuffix;

    InputFormatDetails(String inputFormatType, String fileExtension, String folderSuffix)
    {
      this.inputFormatType = inputFormatType;
      this.fileExtension = fileExtension;
      this.folderSuffix = folderSuffix;
    }

    public String getInputFormatType()
    {
      return inputFormatType;
    }

    public String getFileExtension()
    {
      return fileExtension;
    }

    public String getFolderSuffix()
    {
      return folderSuffix;
    }
  }

  private static final Logger LOG = new Logger(AbstractITBatchIndexTest.class);

  @Inject
  protected IntegrationTestingConfig config;
  @Inject
  protected SqlTestQueryHelper sqlQueryHelper;

  @Inject
  ClientInfoResourceTestClient clientInfoResourceTestClient;

  protected void doIndexTest(
      String dataSource,
      String indexTaskFilePath,
      String queryFilePath,
      boolean waitForNewVersion,
      boolean runTestQueries,
      boolean waitForSegmentsToLoad
  ) throws IOException
  {
    doIndexTest(dataSource, indexTaskFilePath, Function.identity(), queryFilePath, waitForNewVersion, runTestQueries, waitForSegmentsToLoad);
  }

  protected void doIndexTest(
      String dataSource,
      String indexTaskFilePath,
      Function<String, String> taskSpecTransform,
      String queryFilePath,
      boolean waitForNewVersion,
      boolean runTestQueries,
      boolean waitForSegmentsToLoad
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

    submitTaskAndWait(taskSpec, fullDatasourceName, waitForNewVersion, waitForSegmentsToLoad);
    if (runTestQueries) {
      doTestQuery(dataSource, queryFilePath);
    }
  }

  protected void doTestQuery(String dataSource, String queryFilePath)
  {
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
          dataSource + config.getExtraDatasourceNameSuffix()
      );
      queryHelper.testQueriesFromString(queryResponseTemplate);

    }
    catch (Exception e) {
      LOG.error(e, "Error while testing");
      throw new RuntimeException(e);
    }
  }

  protected void doReindexTest(
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

    submitTaskAndWait(taskSpec, fullReindexDatasourceName, false, true);
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

      queryHelper.testQueriesFromString(queryResponseTemplate);
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

    submitTaskAndWait(taskSpec, fullDatasourceName, false, true);
    try {
      sqlQueryHelper.testQueriesFromFile(queryFilePath);
    }
    catch (Exception e) {
      LOG.error(e, "Error while testing");
      throw new RuntimeException(e);
    }
  }

  private void submitTaskAndWait(
      String taskSpec,
      String dataSourceName,
      boolean waitForNewVersion,
      boolean waitForSegmentsToLoad
  )
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

    // IT*ParallelIndexTest do a second round of ingestion to replace segements in an existing
    // data source. For that second round we need to make sure the coordinator actually learned
    // about the new segments befor waiting for it to report that all segments are loaded; otherwise
    // this method could return too early because the coordinator is merely reporting that all the
    // original segments have loaded.
    if (waitForNewVersion) {
      ITRetryUtil.retryUntilTrue(
          () -> {
            final VersionedIntervalTimeline<String, DataSegment> timeline = VersionedIntervalTimeline.forSegments(
                coordinator.getAvailableSegments(dataSourceName)
            );

            final List<TimelineObjectHolder<String, DataSegment>> holders = timeline.lookup(Intervals.ETERNITY);
            return FluentIterable
                .from(holders)
                .transformAndConcat(TimelineObjectHolder::getObject)
                .anyMatch(
                    chunk -> FluentIterable.from(oldVersions)
                                           .anyMatch(oldSegment -> chunk.getObject().overshadows(oldSegment))
                );
          },
          "See a new version"
      );
    }

    if (waitForSegmentsToLoad) {
      ITRetryUtil.retryUntilTrue(
          () -> coordinator.areSegmentsLoaded(dataSourceName), "Segment Load"
      );
    }
  }

  private long countCompleteSubTasks(final String dataSource, final boolean perfectRollup)
  {
    return indexer.getCompleteTasksForDataSource(dataSource)
                  .stream()
                  .filter(t -> {
                    if (!perfectRollup) {
                      return t.getType().equals(SinglePhaseSubTask.TYPE);
                    } else {
                      return t.getType().equalsIgnoreCase(PartialHashSegmentGenerateTask.TYPE)
                             || t.getType().equalsIgnoreCase(PartialDimensionDistributionTask.TYPE)
                             || t.getType().equalsIgnoreCase(PartialDimensionCardinalityTask.TYPE)
                             || t.getType().equalsIgnoreCase(PartialRangeSegmentGenerateTask.TYPE)
                             || t.getType().equalsIgnoreCase(PartialGenericSegmentMergeTask.TYPE);
                    }
                  })
                  .count();
  }

  void verifySegmentsCountAndLoaded(String dataSource, int numExpectedSegments)
  {
    ITRetryUtil.retryUntilTrue(
        () -> coordinator.areSegmentsLoaded(dataSource + config.getExtraDatasourceNameSuffix()),
        "Segment load check"
    );
    ITRetryUtil.retryUntilTrue(
        () -> {
          int segmentCount = coordinator.getAvailableSegments(
              dataSource + config.getExtraDatasourceNameSuffix()
          ).size();
          LOG.info("Current segment count: %d, expected: %d", segmentCount, numExpectedSegments);
          return segmentCount == numExpectedSegments;
        },
        "Segment count check"
    );
  }

  void compactData(String dataSource, String compactionTask) throws Exception
  {
    final String fullDatasourceName = dataSource + config.getExtraDatasourceNameSuffix();
    final List<String> intervalsBeforeCompaction = coordinator.getSegmentIntervals(fullDatasourceName);
    intervalsBeforeCompaction.sort(null);
    final String template = getResourceAsString(compactionTask);
    String taskSpec = StringUtils.replace(template, "%%DATASOURCE%%", fullDatasourceName);

    final String taskID = indexer.submitTask(taskSpec);
    LOG.info("TaskID for compaction task %s", taskID);
    indexer.waitUntilTaskCompletes(taskID);

    ITRetryUtil.retryUntilTrue(
        () -> coordinator.areSegmentsLoaded(fullDatasourceName),
        "Segment Compaction"
    );
    ITRetryUtil.retryUntilTrue(
        () -> {
          final List<String> actualIntervals = coordinator.getSegmentIntervals(
              dataSource + config.getExtraDatasourceNameSuffix()
          );
          actualIntervals.sort(null);
          return actualIntervals.equals(intervalsBeforeCompaction);
        },
        "Compaction interval check"
    );
  }

  void verifySegmentsCompacted(String dataSource, int expectedCompactedSegmentCount)
  {
    List<DataSegment> segments = coordinator.getFullSegmentsMetadata(
        dataSource + config.getExtraDatasourceNameSuffix()
    );
    List<DataSegment> foundCompactedSegments = new ArrayList<>();
    for (DataSegment segment : segments) {
      if (segment.getLastCompactionState() != null) {
        foundCompactedSegments.add(segment);
      }
    }
    Assert.assertEquals(foundCompactedSegments.size(), expectedCompactedSegmentCount);
    for (DataSegment compactedSegment : foundCompactedSegments) {
      Assert.assertNotNull(compactedSegment.getLastCompactionState());
      Assert.assertNotNull(compactedSegment.getLastCompactionState().getPartitionsSpec());
      Assert.assertEquals(
          compactedSegment.getLastCompactionState().getPartitionsSpec().getType(),
          SecondaryPartitionType.LINEAR
      );
    }
  }
}
