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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import org.apache.commons.io.IOUtils;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexer.report.IngestionStatsAndErrors;
import org.apache.druid.indexer.report.IngestionStatsAndErrorsTaskReport;
import org.apache.druid.indexer.report.TaskContextReport;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.indexing.common.task.batch.parallel.PartialDimensionCardinalityTask;
import org.apache.druid.indexing.common.task.batch.parallel.PartialDimensionDistributionTask;
import org.apache.druid.indexing.common.task.batch.parallel.PartialGenericSegmentMergeTask;
import org.apache.druid.indexing.common.task.batch.parallel.PartialHashSegmentGenerateTask;
import org.apache.druid.indexing.common.task.batch.parallel.PartialRangeSegmentGenerateTask;
import org.apache.druid.indexing.common.task.batch.parallel.SinglePhaseSubTask;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.http.SqlTaskStatus;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.druid.testing.embedded.msq.EmbeddedMSQApis;
import org.apache.druid.testing.tools.ITRetryUtil;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentTimeline;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.junit.Assert;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  /**
   * Reads file as utf-8 string and replace %%DATASOURCE%% with the provide datasource value.
   */
  public String getStringFromFileAndReplaceDatasource(String filePath, String datasource)
  {
    String fileString;
    try {
      InputStream is = AbstractITBatchIndexTest.class.getResourceAsStream(filePath);
      fileString = IOUtils.toString(is, StandardCharsets.UTF_8);
    }
    catch (IOException e) {
      throw new ISE(e, "could not read query file: %s", filePath);
    }

    fileString = StringUtils.replace(fileString, PlaceHolders.DATASOURCE, datasource);
    fileString = StringUtils.replace(fileString, PlaceHolders.DATA_DIRECTORY, getDataDirectory());

    return fileString;
  }

  protected void doTestQuery(String dataSource, String queryFilePath)
  {
    doTestQuery(dataSource, queryFilePath, false);
  }

  /**
   * Reads native queries from a file and runs against the provided datasource.
   */
  protected void doTestQuery(String dataSource, String queryFilePath, boolean isSql)
  {
    try {
      if (isSql) {
        queryResultsVerifier.testSqlQueriesFromResource(queryFilePath, dataSource);
      } else {
        queryResultsVerifier.testNativeQueriesFromResource(queryFilePath, dataSource);
      }
    }
    catch (Exception e) {
      LOG.error(e, "Error while running test query at path " + queryFilePath);
      throw new RuntimeException(e);
    }
  }

  /**
   * Submits a sqlTask, waits for task completion.
   */
  protected void submitMSQTask(String sqlTask, String datasource, Map<String, Object> msqContext)
  {
    // Submit the tasks and wait for the datasource to get loaded
    final EmbeddedMSQApis msqApis = new EmbeddedMSQApis(cluster, overlord);
    final SqlTaskStatus taskStatus = msqApis.submitTaskSql(msqContext, sqlTask);
    cluster.callApi().waitForTaskToSucceed(taskStatus.getTaskId(), overlord.latchableEmitter());

    cluster.callApi().waitForAllSegmentsToBeAvailable(datasource, coordinator, broker);
  }

  /**
   * Submits a sqlTask, waits for task completion.
   */
  protected void submitMSQTaskFromFile(String sqlFilePath, String datasource, Map<String, Object> msqContext) throws Exception
  {
    String sqlTask = getStringFromFileAndReplaceDatasource(sqlFilePath, datasource);
    submitMSQTask(sqlTask, datasource, msqContext);
  }

  /**
   * Runs a SQL ingest test.
   *
   * @param  sqlFilePath path of file containing the sql query.
   * @param  queryFilePath path of file containing the native test queries to be run on the ingested datasource.
   * @param  datasource name of the datasource. %%DATASOURCE%% in the sql and queries will be replaced with this value.
   * @param  msqContext context parameters to be passed with MSQ API call.
   */
  protected void runMSQTaskandTestQueries(String sqlFilePath,
                                          String queryFilePath,
                                          String datasource,
                                          Map<String, Object> msqContext
  ) throws Exception
  {
    LOG.info("Starting MSQ test for sql path: %s, query path: %s]", sqlFilePath, queryFilePath);

    submitMSQTaskFromFile(sqlFilePath, datasource, msqContext);
    doTestQuery(datasource, queryFilePath);
  }

  /**
   * Runs a reindex SQL ingest test.
   * Same as runMSQTaskandTestQueries, but replaces both %%DATASOURCE%% and %%REINDEX_DATASOURCE%% in the SQL Task.
   */
  protected void runReindexMSQTaskandTestQueries(String sqlFilePath,
                                                 String queryFilePath,
                                                 String datasource,
                                                 String reindexDatasource,
                                                 Map<String, Object> msqContext
  ) throws Exception
  {
    LOG.info("Starting Reindex MSQ test for sql path: %s, query path: %s", sqlFilePath, queryFilePath);

    String sqlTask = getStringFromFileAndReplaceDatasource(sqlFilePath, datasource);
    sqlTask = replaceDataDirectoryInSpec(sqlTask);
    sqlTask = StringUtils.replace(
        sqlTask,
        "%%REINDEX_DATASOURCE%%",
        reindexDatasource
    );
    submitMSQTask(sqlTask, reindexDatasource, msqContext);
    doTestQuery(reindexDatasource, queryFilePath);
  }

  protected void doIndexTest(
      String dataSource,
      String indexTaskFilePath,
      String queryFilePath,
      boolean waitForNewVersion,
      boolean runTestQueries,
      boolean waitForSegmentsToLoad,
      Pair<Boolean, Boolean> segmentAvailabilityConfirmationPair
  ) throws IOException
  {
    doIndexTest(
        dataSource,
        indexTaskFilePath,
        Function.identity(),
        queryFilePath,
        waitForNewVersion,
        runTestQueries,
        false,
        waitForSegmentsToLoad,
        segmentAvailabilityConfirmationPair
    );
  }

  protected void doIndexTest(
      String dataSource,
      String indexTaskFilePath,
      Function<String, String> taskSpecTransform,
      String queryFilePath,
      boolean waitForNewVersion,
      boolean runTestQueries,
      boolean waitForSegmentsToLoad,
      Pair<Boolean, Boolean> segmentAvailabilityConfirmationPair
  ) throws IOException
  {
    doIndexTest(
        dataSource,
        indexTaskFilePath,
        taskSpecTransform,
        queryFilePath,
        waitForNewVersion,
        runTestQueries,
        false,
        waitForSegmentsToLoad,
        segmentAvailabilityConfirmationPair
    );
  }

  protected void doIndexTest(
      String dataSource,
      String indexTaskFilePath,
      Function<String, String> taskSpecTransform,
      String queryFilePath,
      boolean waitForNewVersion,
      boolean runTestQueries,
      boolean isSqlQueries,
      boolean waitForSegmentsToLoad,
      Pair<Boolean, Boolean> segmentAvailabilityConfirmationPair
  ) throws IOException
  {
    String taskSpec = taskSpecTransform.apply(
        StringUtils.replace(
            getResourceAsString(indexTaskFilePath),
            PlaceHolders.DATASOURCE,
            dataSource
        )
    );
    taskSpec = replaceDataDirectoryInSpec(taskSpec);

    submitTaskAndWait(
        taskSpec,
        dataSource,
        waitForNewVersion,
        waitForSegmentsToLoad,
        segmentAvailabilityConfirmationPair
    );
    if (runTestQueries) {
      doTestQuery(dataSource, queryFilePath, isSqlQueries);
    }
  }

  protected void doReindexTest(
      String baseDataSource,
      String reindexDataSource,
      String reindexTaskFilePath,
      String queryFilePath,
      Pair<Boolean, Boolean> segmentAvailabilityConfirmationPair
  ) throws IOException
  {
    doReindexTest(
        baseDataSource,
        reindexDataSource,
        Function.identity(),
        reindexTaskFilePath,
        queryFilePath,
        segmentAvailabilityConfirmationPair
    );
  }

  void doReindexTest(
      String baseDataSource,
      String reindexDataSource,
      Function<String, String> taskSpecTransform,
      String reindexTaskFilePath,
      String queryFilePath,
      Pair<Boolean, Boolean> segmentAvailabilityConfirmationPair
  ) throws IOException
  {
    String taskSpec = StringUtils.replace(
        getResourceAsString(reindexTaskFilePath),
        PlaceHolders.DATASOURCE,
        baseDataSource
    );
    taskSpec = replaceDataDirectoryInSpec(taskSpec);
    taskSpec = StringUtils.replace(
        taskSpec,
        "%%REINDEX_DATASOURCE%%",
        reindexDataSource
    );

    taskSpec = taskSpecTransform.apply(taskSpec);

    submitTaskAndWait(
        taskSpec,
        reindexDataSource,
        false,
        true,
        segmentAvailabilityConfirmationPair
    );
    try {
      queryResultsVerifier.testNativeQueriesFromResource(queryFilePath, reindexDataSource);
      // verify excluded dimension is not reIndexed
      final String url = StringUtils.format(
          "/druid/v2/datasources/%s/dimensions?interval=%s",
          reindexDataSource,
          "2013-08-31T00:00:00.000Z/2013-09-10T00:00:00.000Z"
      );
      final List<String> dimensions = cluster.callApi().serviceClient().onAnyBroker(
          mapper -> new RequestBuilder(HttpMethod.GET, url),
          new TypeReference<>(){}
      );
      Assert.assertFalse("dimensions : " + dimensions, dimensions.contains("robot"));
    }
    catch (Exception e) {
      LOG.error(e, "Error while testing");
      throw new RuntimeException(e);
    }
  }

  void doIndexTestSqlTest(
      String dataSource,
      String indexTaskFilePath,
      String queryFilePath,
      Function<String, String> taskSpecTransform
  ) throws IOException
  {
    String taskSpec = taskSpecTransform.apply(
        StringUtils.replace(
            getResourceAsString(indexTaskFilePath),
            PlaceHolders.DATASOURCE,
            dataSource
        )
    );
    taskSpec = replaceDataDirectoryInSpec(taskSpec);

    Pair<Boolean, Boolean> dummyPair = new Pair<>(false, false);
    submitTaskAndWait(taskSpec, dataSource, false, true, dummyPair);
    try {
      queryResultsVerifier.testSqlQueriesFromResource(queryFilePath, dataSource);
    }
    catch (Exception e) {
      LOG.error(e, "Error while testing");
      throw new RuntimeException(e);
    }
  }

  protected void submitTaskAndWait(
      String taskSpec,
      String dataSourceName,
      boolean waitForNewVersion,
      boolean waitForSegmentsToLoad,
      Pair<Boolean, Boolean> segmentAvailabilityConfirmationPair
  )
  {
    // Wait for any existing kill tasks to complete before submitting new index task otherwise
    // kill tasks can fail with interval lock revoked.
    waitForAllTasksToCompleteForDataSource(dataSourceName);
    final Collection<DataSegment> oldVersions = waitForNewVersion
                                                ? cluster.callApi().getVisibleUsedSegments(dataSourceName, overlord)
                                                : null;

    long startSubTaskCount = -1;
    final boolean assertRunsSubTasks = taskSpec.contains("index_parallel");
    if (assertRunsSubTasks) {
      startSubTaskCount = countCompleteSubTasks(dataSourceName, !taskSpec.contains("dynamic"));
    }

    LOG.info("Submitting the following spec for ingestion - \n%s", taskSpec);
    final String taskID = submitTask(taskSpec);
    LOG.info("TaskID for loading index task %s", taskID);
    cluster.callApi().waitForTaskToSucceed(taskID, overlord);

    if (assertRunsSubTasks) {
      final boolean perfectRollup = !taskSpec.contains("dynamic");
      final long newSubTasks = countCompleteSubTasks(dataSourceName, perfectRollup) - startSubTaskCount;
      Assert.assertTrue(
          StringUtils.format(
              "The supervisor task [%s] didn't create any sub tasks. Was it executed in the parallel mode?",
              taskID
          ),
          newSubTasks > 0
      );
    }

    if (segmentAvailabilityConfirmationPair.lhs != null && segmentAvailabilityConfirmationPair.lhs) {
      TaskReport reportRaw = cluster.callApi().onLeaderOverlord(o -> o.taskReportAsMap(taskID)).get("ingestionStatsAndErrors");
      IngestionStatsAndErrorsTaskReport report = (IngestionStatsAndErrorsTaskReport) reportRaw;
      IngestionStatsAndErrors reportData = report.getPayload();

      // Confirm that the task waited longer than 0ms for the task to complete.
      Assert.assertTrue(reportData.getSegmentAvailabilityWaitTimeMs() > 0);

      // Make sure that the result of waiting for segments to load matches the expected result
      if (segmentAvailabilityConfirmationPair.rhs != null) {
        Assert.assertEquals(
            reportData.isSegmentAvailabilityConfirmed(),
            segmentAvailabilityConfirmationPair.rhs
        );
      }

      TaskContextReport taskContextReport =
          (TaskContextReport) cluster.callApi()
                                     .onLeaderOverlord(o -> o.taskReportAsMap(taskID))
                                     .get(TaskContextReport.REPORT_KEY);

      Assert.assertFalse(taskContextReport.getPayload().isEmpty());
    }

    // IT*ParallelIndexTest do a second round of ingestion to replace segments in an existing
    // data source. For that second round we need to make sure the coordinator actually learned
    // about the new segments before waiting for it to report that all segments are loaded; otherwise
    // this method could return too early because the coordinator is merely reporting that all the
    // original segments have loaded.
    if (waitForNewVersion) {
      ITRetryUtil.retryUntilTrue(
          () -> {
            final SegmentTimeline timeline = SegmentTimeline.forSegments(
                cluster.callApi().getVisibleUsedSegments(dataSourceName, overlord)
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
      cluster.callApi().waitForAllSegmentsToBeAvailable(dataSourceName, coordinator, broker);
    }
  }

  private long countCompleteSubTasks(final String dataSource, final boolean perfectRollup)
  {
    return ImmutableList
        .copyOf(
            (CloseableIterator<TaskStatusPlus>)
                cluster.callApi().onLeaderOverlord(
                    o -> o.taskStatuses("complete", dataSource, Integer.MAX_VALUE)
                )
        )
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

  void verifySegmentsCountAndLoaded(String dataSource, int numExpectedSegments, int numExpectedTombstones)
  {
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    ITRetryUtil.retryUntilTrue(
        () -> {
          Set<DataSegment> segments = cluster.callApi().getVisibleUsedSegments(dataSource, overlord);
          int segmentCount = segments.size();
          LOG.info("Current segment count: %d, expected: %d", segmentCount, numExpectedSegments);

          int tombstoneCount = 0;
          for (DataSegment segment : segments) {
            if (segment.isTombstone()) {
              tombstoneCount++;
            }
          }

          LOG.info("Current tombstone count: %d, expected: %d", tombstoneCount, numExpectedTombstones);

          return segmentCount == numExpectedSegments && tombstoneCount == numExpectedTombstones;
        },
        "Segment count check"
    );
  }

  protected String submitIndexTask(String indexTaskResourceName, final String fullDatasourceName) throws Exception
  {
    // Wait for any existing kill tasks to complete before submitting new index task otherwise
    // kill tasks can fail with interval lock revoked.
    waitForAllTasksToCompleteForDataSource(fullDatasourceName);
    final String taskSpecTemplate = getResourceAsString(indexTaskResourceName);
    String taskSpec = StringUtils.replace(taskSpecTemplate, PlaceHolders.DATASOURCE, fullDatasourceName);
    taskSpec = replaceDataDirectoryInSpec(taskSpec);
    taskSpec = StringUtils.replace(
      taskSpec,
      "%%SEGMENT_AVAIL_TIMEOUT_MILLIS%%",
      jsonMapper.writeValueAsString("0")
  );

    return submitTask(taskSpec);
  }

  /**
   * @return Absolute path of the {@code data} resource directory.
   */
  protected static String getDataDirectory()
  {
    return Resources.getFileForResource("data").getAbsolutePath();
  }

  protected String replaceDataDirectoryInSpec(String spec)
  {
    return StringUtils.replace(spec, PlaceHolders.DATA_DIRECTORY, getDataDirectory());
  }
}
