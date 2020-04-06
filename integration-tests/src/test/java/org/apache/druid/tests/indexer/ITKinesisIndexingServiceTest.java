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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManager;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.DruidClusterAdminClient;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.testing.utils.KinesisAdminClient;
import org.apache.druid.testing.utils.KinesisEventWriter;
import org.apache.druid.testing.utils.WikipediaStreamEventStreamGenerator;
import org.apache.druid.tests.TestNGGroup;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

@Test(groups = TestNGGroup.KINESIS_INDEX)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITKinesisIndexingServiceTest extends AbstractITBatchIndexTest
{
  private static final Logger LOG = new Logger(ITKinesisIndexingServiceTest.class);
  private static final int KINESIS_SHARD_COUNT = 2;
  // Since this integration test can terminates or be killed un-expectedly, this tag is added to all streams created
  // to help make stream clean up easier. (Normally, streams should be cleanup automattically by the teardown method)
  // The value to this tag is a timestamp that can be used by a lambda function to remove unused stream.
  private static final String STREAM_EXPIRE_TAG = "druid-ci-expire-after";
  private static final long WAIT_TIME_MILLIS = 3 * 60 * 1000L;
  private static final DateTime FIRST_EVENT_TIME = DateTimes.of(1994, 4, 29, 1, 0);
  private static final String INDEXER_FILE_LEGACY_PARSER = "/indexer/stream_supervisor_spec_legacy_parser.json";
  private static final String INDEXER_FILE_INPUT_FORMAT = "/indexer/stream_supervisor_spec_input_format.json";
  private static final String QUERIES_FILE = "/indexer/stream_index_queries.json";
  // format for the querying interval
  private static final DateTimeFormatter INTERVAL_FMT = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:'00Z'");
  // format for the expected timestamp in a query response
  private static final DateTimeFormatter TIMESTAMP_FMT = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'.000Z'");
  private static final int EVENTS_PER_SECOND = 6;
  private static final long CYCLE_PADDING_MS = 100;
  private static final int TOTAL_NUMBER_OF_SECOND = 10;

  @Inject
  private DruidClusterAdminClient druidClusterAdminClient;

  private String streamName;
  private String fullDatasourceName;
  private KinesisAdminClient kinesisAdminClient;
  private KinesisEventWriter kinesisEventWriter;
  private WikipediaStreamEventStreamGenerator wikipediaStreamEventGenerator;
  private Function<String, String> kinesisIngestionPropsTransform;
  private Function<String, String> kinesisQueryPropsTransform;
  private String supervisorId;
  private int secondsToGenerateRemaining;

  @BeforeClass
  public void beforeClass() throws Exception
  {
    kinesisAdminClient = new KinesisAdminClient(config.getStreamEndpoint());
    kinesisEventWriter = new KinesisEventWriter(config.getStreamEndpoint(), false);
    wikipediaStreamEventGenerator = new WikipediaStreamEventStreamGenerator(EVENTS_PER_SECOND, CYCLE_PADDING_MS);
  }

  @AfterClass
  public void tearDown()
  {
    wikipediaStreamEventGenerator.shutdown();
    kinesisEventWriter.shutdown();
  }

  @BeforeMethod
  public void before()
  {
    streamName = "kinesis_index_test_" + UUID.randomUUID();
    String datasource = "kinesis_indexing_service_test_" + UUID.randomUUID();
    Map<String, String> tags = ImmutableMap.of(STREAM_EXPIRE_TAG, Long.toString(DateTimes.nowUtc().plusMinutes(30).getMillis()));
    kinesisAdminClient.createStream(streamName, KINESIS_SHARD_COUNT, tags);
    ITRetryUtil.retryUntil(
        () -> kinesisAdminClient.isStreamActive(streamName),
        true,
        10000,
        30,
        "Wait for stream active"
    );
    secondsToGenerateRemaining = TOTAL_NUMBER_OF_SECOND;
    fullDatasourceName = datasource + config.getExtraDatasourceNameSuffix();
    kinesisIngestionPropsTransform = spec -> {
      try {
        spec = StringUtils.replace(
            spec,
            "%%DATASOURCE%%",
            fullDatasourceName
        );
        spec = StringUtils.replace(
            spec,
            "%%STREAM_TYPE%%",
            "kinesis"
        );
        spec = StringUtils.replace(
            spec,
            "%%TOPIC_KEY%%",
            "stream"
        );
        spec = StringUtils.replace(
            spec,
            "%%TOPIC_VALUE%%",
            streamName
        );
        spec = StringUtils.replace(
            spec,
            "%%USE_EARLIEST_KEY%%",
            "useEarliestSequenceNumber"
        );
        spec = StringUtils.replace(
            spec,
            "%%STREAM_PROPERTIES_KEY%%",
            "endpoint"
        );
        return StringUtils.replace(
            spec,
            "%%STREAM_PROPERTIES_VALUE%%",
            jsonMapper.writeValueAsString(config.getStreamEndpoint())
        );
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
    kinesisQueryPropsTransform = spec -> {
      try {
        spec = StringUtils.replace(
            spec,
            "%%DATASOURCE%%",
            fullDatasourceName
        );
        spec = StringUtils.replace(
            spec,
            "%%TIMEBOUNDARY_RESPONSE_TIMESTAMP%%",
            TIMESTAMP_FMT.print(FIRST_EVENT_TIME)
        );
        spec = StringUtils.replace(
            spec,
            "%%TIMEBOUNDARY_RESPONSE_MAXTIME%%",
            TIMESTAMP_FMT.print(FIRST_EVENT_TIME.plusSeconds(TOTAL_NUMBER_OF_SECOND - 1))
        );
        spec = StringUtils.replace(
            spec,
            "%%TIMEBOUNDARY_RESPONSE_MINTIME%%",
            TIMESTAMP_FMT.print(FIRST_EVENT_TIME)
        );
        spec = StringUtils.replace(
            spec,
            "%%TIMESERIES_QUERY_START%%",
            INTERVAL_FMT.print(FIRST_EVENT_TIME)
        );
        spec = StringUtils.replace(
            spec,
            "%%TIMESERIES_QUERY_END%%",
            INTERVAL_FMT.print(FIRST_EVENT_TIME.plusSeconds(TOTAL_NUMBER_OF_SECOND - 1).plusMinutes(2))
        );
        spec = StringUtils.replace(
            spec,
            "%%TIMESERIES_RESPONSE_TIMESTAMP%%",
            TIMESTAMP_FMT.print(FIRST_EVENT_TIME)
        );
        spec = StringUtils.replace(
            spec,
            "%%TIMESERIES_ADDED%%",
            Long.toString(getSumOfEventSequence(EVENTS_PER_SECOND) * TOTAL_NUMBER_OF_SECOND)
        );
        return StringUtils.replace(
            spec,
            "%%TIMESERIES_NUMEVENTS%%",
            Integer.toString(EVENTS_PER_SECOND * TOTAL_NUMBER_OF_SECOND)
        );
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }

  @AfterMethod
  public void teardown()
  {
    try {
      kinesisEventWriter.flush();
      indexer.shutdownSupervisor(supervisorId);
    }
    catch (Exception e) {
      // Best effort cleanup as the supervisor may have already went Bye-Bye
    }
    try {
      unloader(fullDatasourceName);
    }
    catch (Exception e) {
      // Best effort cleanup as the datasource may have already went Bye-Bye
    }
    try {
      kinesisAdminClient.deleteStream(streamName);
    }
    catch (Exception e) {
      // Best effort cleanup as the stream may have already went Bye-Bye
    }
  }

  @Test
  public void testKinesisIndexDataWithLegacyParserStableState() throws Exception
  {
    try (
        final Closeable ignored1 = unloader(fullDatasourceName)
    ) {
      final String taskSpec = kinesisIngestionPropsTransform.apply(getResourceAsString(INDEXER_FILE_LEGACY_PARSER));
      LOG.info("supervisorSpec: [%s]\n", taskSpec);
      // Start supervisor
      supervisorId = indexer.submitSupervisor(taskSpec);
      LOG.info("Submitted supervisor");
      // Start Kinesis data generator
      wikipediaStreamEventGenerator.start(streamName, kinesisEventWriter, TOTAL_NUMBER_OF_SECOND, FIRST_EVENT_TIME);
      verifyIngestedData(supervisorId);
    }
  }

  @Test
  public void testKinesisIndexDataWithInputFormatStableState() throws Exception
  {
    try (
        final Closeable ignored1 = unloader(fullDatasourceName)
    ) {
      final String taskSpec = kinesisIngestionPropsTransform.apply(getResourceAsString(INDEXER_FILE_INPUT_FORMAT));
      LOG.info("supervisorSpec: [%s]\n", taskSpec);
      // Start supervisor
      supervisorId = indexer.submitSupervisor(taskSpec);
      LOG.info("Submitted supervisor");
      // Start Kinesis data generator
      wikipediaStreamEventGenerator.start(streamName, kinesisEventWriter, TOTAL_NUMBER_OF_SECOND, FIRST_EVENT_TIME);
      verifyIngestedData(supervisorId);
    }
  }

  @Test
  public void testKinesisIndexDataWithLosingCoordinator() throws Exception
  {
    testIndexWithLosingNodeHelper(() -> druidClusterAdminClient.restartCoordinatorContainer(), () -> druidClusterAdminClient.waitUntilCoordinatorReady());
  }

  @Test
  public void testKinesisIndexDataWithLosingOverlord() throws Exception
  {
    testIndexWithLosingNodeHelper(() -> druidClusterAdminClient.restartIndexerContainer(), () -> druidClusterAdminClient.waitUntilIndexerReady());
  }

  @Test
  public void testKinesisIndexDataWithLosingHistorical() throws Exception
  {
    testIndexWithLosingNodeHelper(() -> druidClusterAdminClient.restartHistoricalContainer(), () -> druidClusterAdminClient.waitUntilHistoricalReady());
  }

  @Test
  public void testKinesisIndexDataWithStartStopSupervisor() throws Exception
  {
    try (
        final Closeable ignored1 = unloader(fullDatasourceName)
    ) {
      final String taskSpec = kinesisIngestionPropsTransform.apply(getResourceAsString(INDEXER_FILE_INPUT_FORMAT));
      LOG.info("supervisorSpec: [%s]\n", taskSpec);
      // Start supervisor
      supervisorId = indexer.submitSupervisor(taskSpec);
      LOG.info("Submitted supervisor");
      // Start generating half of the data
      int secondsToGenerateFirstRound = TOTAL_NUMBER_OF_SECOND / 2;
      secondsToGenerateRemaining = secondsToGenerateRemaining - secondsToGenerateFirstRound;
      wikipediaStreamEventGenerator.start(streamName, kinesisEventWriter, secondsToGenerateFirstRound, FIRST_EVENT_TIME);
      // Verify supervisor is healthy before suspension
      ITRetryUtil.retryUntil(
          () -> SupervisorStateManager.BasicState.RUNNING.equals(indexer.getSupervisorStatus(supervisorId)),
          true,
          10000,
          30,
          "Waiting for supervisor to be healthy"
      );
      // Suspend the supervisor
      indexer.suspendSupervisor(supervisorId);
      // Start generating remainning half of the data
      wikipediaStreamEventGenerator.start(streamName, kinesisEventWriter, secondsToGenerateRemaining, FIRST_EVENT_TIME.plusSeconds(secondsToGenerateFirstRound));
      // Resume the supervisor
      indexer.resumeSupervisor(supervisorId);
      // Verify supervisor is healthy after suspension
      ITRetryUtil.retryUntil(
          () -> SupervisorStateManager.BasicState.RUNNING.equals(indexer.getSupervisorStatus(supervisorId)),
          true,
          10000,
          30,
          "Waiting for supervisor to be healthy"
      );
      // Verify that supervisor can catch up with the stream
      verifyIngestedData(supervisorId);
    }
  }

  @Test
  public void testKinesisIndexDataWithKinesisReshardSplit() throws Exception
  {
    // Reshard the supervisor by split from KINESIS_SHARD_COUNT to KINESIS_SHARD_COUNT * 2
    testIndexWithKinesisReshardHelper(KINESIS_SHARD_COUNT * 2);
  }

  @Test
  public void testKinesisIndexDataWithKinesisReshardMerge() throws Exception
  {
    // Reshard the supervisor by split from KINESIS_SHARD_COUNT to KINESIS_SHARD_COUNT / 2
    testIndexWithKinesisReshardHelper(KINESIS_SHARD_COUNT / 2);
  }

  private void testIndexWithLosingNodeHelper(Runnable restartRunnable, Runnable waitForReadyRunnable) throws Exception
  {
    try (
        final Closeable ignored1 = unloader(fullDatasourceName)
    ) {
      final String taskSpec = kinesisIngestionPropsTransform.apply(getResourceAsString(INDEXER_FILE_INPUT_FORMAT));
      LOG.info("supervisorSpec: [%s]\n", taskSpec);
      // Start supervisor
      supervisorId = indexer.submitSupervisor(taskSpec);
      LOG.info("Submitted supervisor");
      // Start generating one third of the data (before restarting)
      int secondsToGenerateFirstRound = TOTAL_NUMBER_OF_SECOND / 3;
      secondsToGenerateRemaining = secondsToGenerateRemaining - secondsToGenerateFirstRound;
      wikipediaStreamEventGenerator.start(streamName, kinesisEventWriter, secondsToGenerateFirstRound, FIRST_EVENT_TIME);
      // Verify supervisor is healthy before restart
      ITRetryUtil.retryUntil(
          () -> SupervisorStateManager.BasicState.RUNNING.equals(indexer.getSupervisorStatus(supervisorId)),
          true,
          10000,
          30,
          "Waiting for supervisor to be healthy"
      );
      // Restart Druid process
      LOG.info("Restarting Druid process");
      restartRunnable.run();
      LOG.info("Restarted Druid process");
      // Start generating one third of the data (while restarting)
      int secondsToGenerateSecondRound = TOTAL_NUMBER_OF_SECOND / 3;
      secondsToGenerateRemaining = secondsToGenerateRemaining - secondsToGenerateSecondRound;
      wikipediaStreamEventGenerator.start(streamName, kinesisEventWriter, secondsToGenerateSecondRound, FIRST_EVENT_TIME.plusSeconds(secondsToGenerateFirstRound));
      // Wait for Druid process to be available
      LOG.info("Waiting for Druid process to be available");
      waitForReadyRunnable.run();
      LOG.info("Druid process is now available");
      // Start generating remainding data (after restarting)
      wikipediaStreamEventGenerator.start(streamName, kinesisEventWriter, secondsToGenerateRemaining, FIRST_EVENT_TIME.plusSeconds(secondsToGenerateFirstRound + secondsToGenerateSecondRound));
      // Verify supervisor is healthy
      ITRetryUtil.retryUntil(
          () -> SupervisorStateManager.BasicState.RUNNING.equals(indexer.getSupervisorStatus(supervisorId)),
          true,
          10000,
          30,
          "Waiting for supervisor to be healthy"
      );
      // Verify that supervisor ingested all data
      verifyIngestedData(supervisorId);
    }
  }

  private void testIndexWithKinesisReshardHelper(int newShardCount) throws Exception
  {
    try (
        final Closeable ignored1 = unloader(fullDatasourceName)
    ) {
      final String taskSpec = kinesisIngestionPropsTransform.apply(getResourceAsString(INDEXER_FILE_INPUT_FORMAT));
      LOG.info("supervisorSpec: [%s]\n", taskSpec);
      // Start supervisor
      supervisorId = indexer.submitSupervisor(taskSpec);
      LOG.info("Submitted supervisor");
      // Start generating one third of the data (before resharding)
      int secondsToGenerateFirstRound = TOTAL_NUMBER_OF_SECOND / 3;
      secondsToGenerateRemaining = secondsToGenerateRemaining - secondsToGenerateFirstRound;
      wikipediaStreamEventGenerator.start(streamName, kinesisEventWriter, secondsToGenerateFirstRound, FIRST_EVENT_TIME);
      // Verify supervisor is healthy before resahrding
      ITRetryUtil.retryUntil(
          () -> SupervisorStateManager.BasicState.RUNNING.equals(indexer.getSupervisorStatus(supervisorId)),
          true,
          10000,
          30,
          "Waiting for supervisor to be healthy"
      );
      // Reshard the supervisor by split from KINESIS_SHARD_COUNT to newShardCount and waits until the resharding starts
      kinesisAdminClient.updateShardCount(streamName, newShardCount, true);
      // Start generating one third of the data (while resharding)
      int secondsToGenerateSecondRound = TOTAL_NUMBER_OF_SECOND / 3;
      secondsToGenerateRemaining = secondsToGenerateRemaining - secondsToGenerateSecondRound;
      wikipediaStreamEventGenerator.start(streamName, kinesisEventWriter, secondsToGenerateSecondRound, FIRST_EVENT_TIME.plusSeconds(secondsToGenerateFirstRound));
      // Wait for kinesis stream to finish resharding
      ITRetryUtil.retryUntil(
          () -> kinesisAdminClient.isStreamActive(streamName),
          true,
          10000,
          30,
          "Waiting for Kinesis stream to finish resharding"
      );
      // Start generating remainding data (after resharding)
      wikipediaStreamEventGenerator.start(streamName, kinesisEventWriter, secondsToGenerateRemaining, FIRST_EVENT_TIME.plusSeconds(secondsToGenerateFirstRound + secondsToGenerateSecondRound));
      // Verify supervisor is healthy after resahrding
      ITRetryUtil.retryUntil(
          () -> SupervisorStateManager.BasicState.RUNNING.equals(indexer.getSupervisorStatus(supervisorId)),
          true,
          10000,
          30,
          "Waiting for supervisor to be healthy"
      );
      // Verify that supervisor can catch up with the stream
      verifyIngestedData(supervisorId);
    }
  }

  private void verifyIngestedData(String supervisorId) throws Exception
  {
    // Wait for supervisor to consume events
    LOG.info("Waiting for [%s] millis for Kinesis indexing tasks to consume events", WAIT_TIME_MILLIS);
    Thread.sleep(WAIT_TIME_MILLIS);
    // Query data
    final String querySpec = kinesisQueryPropsTransform.apply(getResourceAsString(QUERIES_FILE));
    // this query will probably be answered from the indexing tasks but possibly from 2 historical segments / 2 indexing
    this.queryHelper.testQueriesFromString(querySpec, 2);
    LOG.info("Shutting down supervisor");
    indexer.shutdownSupervisor(supervisorId);
    // wait for all Kinesis indexing tasks to finish
    LOG.info("Waiting for all indexing tasks to finish");
    ITRetryUtil.retryUntilTrue(
        () -> (indexer.getPendingTasks().size()
               + indexer.getRunningTasks().size()
               + indexer.getWaitingTasks().size()) == 0,
        "Waiting for Tasks Completion"
    );
    // wait for segments to be handed off
    ITRetryUtil.retryUntil(
        () -> coordinator.areSegmentsLoaded(fullDatasourceName),
        true,
        10000,
        30,
        "Real-time generated segments loaded"
    );

    // this query will be answered by at least 1 historical segment, most likely 2, and possibly up to all 4
    this.queryHelper.testQueriesFromString(querySpec, 2);
  }
  private long getSumOfEventSequence(int numEvents)
  {
    return (numEvents * (1 + numEvents)) / 2;
  }
}
