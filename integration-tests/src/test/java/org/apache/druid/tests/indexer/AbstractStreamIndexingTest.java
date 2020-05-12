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
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManager;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.utils.DruidClusterAdminClient;
import org.apache.druid.testing.utils.EventSerializer;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.testing.utils.JsonEventSerializer;
import org.apache.druid.testing.utils.StreamAdminClient;
import org.apache.druid.testing.utils.StreamEventWriter;
import org.apache.druid.testing.utils.StreamGenerator;
import org.apache.druid.testing.utils.WikipediaStreamEventStreamGenerator;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class AbstractStreamIndexingTest extends AbstractIndexerTest
{
  static final DateTime FIRST_EVENT_TIME = DateTimes.of(1994, 4, 29, 1, 0);
  // format for the querying interval
  static final DateTimeFormatter INTERVAL_FMT = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:'00Z'");
  // format for the expected timestamp in a query response
  static final DateTimeFormatter TIMESTAMP_FMT = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'.000Z'");
  static final int EVENTS_PER_SECOND = 6;
  static final int TOTAL_NUMBER_OF_SECOND = 10;

  private static final Logger LOG = new Logger(AbstractStreamIndexingTest.class);
  // Since this integration test can terminates or be killed un-expectedly, this tag is added to all streams created
  // to help make stream clean up easier. (Normally, streams should be cleanup automattically by the teardown method)
  // The value to this tag is a timestamp that can be used by a lambda function to remove unused stream.
  private static final String STREAM_EXPIRE_TAG = "druid-ci-expire-after";
  private static final int STREAM_SHARD_COUNT = 2;
  private static final long WAIT_TIME_MILLIS = 3 * 60 * 1000L;
  private static final long CYCLE_PADDING_MS = 100;

  private static final String QUERIES_FILE = "/stream/queries/stream_index_queries.json";
  private static final String SUPERVISOR_SPEC_TEMPLATE_FILE = "supervisor_spec_template.json";

  protected static final String DATA_RESOURCE_ROOT = "/stream/data";
  protected static final String SUPERVISOR_SPEC_TEMPLATE_PATH =
      String.join("/", DATA_RESOURCE_ROOT, SUPERVISOR_SPEC_TEMPLATE_FILE);
  protected static final String SERIALIZER_SPEC_DIR = "serializer";
  protected static final String INPUT_FORMAT_SPEC_DIR = "input_format";
  protected static final String INPUT_ROW_PARSER_SPEC_DIR = "parser";

  protected static final String SERIALIZER = "serializer";
  protected static final String INPUT_FORMAT = "inputFormat";
  protected static final String INPUT_ROW_PARSER = "parser";

  private static final String JSON_INPUT_FORMAT_PATH =
      String.join("/", DATA_RESOURCE_ROOT, "json", INPUT_FORMAT_SPEC_DIR, "input_format.json");

  @Inject
  private DruidClusterAdminClient druidClusterAdminClient;

  @Inject
  private IntegrationTestingConfig config;

  private StreamAdminClient streamAdminClient;

  abstract StreamAdminClient createStreamAdminClient(IntegrationTestingConfig config) throws Exception;

  /**
   * Create an event writer for an underlying stream. {@code transactionEnabled} should not be null if the stream
   * supports transactions. It is ignored otherwise.
   */
  abstract StreamEventWriter createStreamEventWriter(
      IntegrationTestingConfig config,
      @Nullable Boolean transactionEnabled
  ) throws Exception;

  abstract Function<String, String> generateStreamIngestionPropsTransform(
      String streamName,
      String fullDatasourceName,
      String parserType,
      String parserOrInputFormat,
      IntegrationTestingConfig config
  );

  abstract Function<String, String> generateStreamQueryPropsTransform(String streamName, String fullDatasourceName);

  public abstract String getTestNamePrefix();

  protected void doBeforeClass() throws Exception
  {
    streamAdminClient = createStreamAdminClient(config);
  }

  private static String getOnlyResourcePath(String resourceRoot) throws IOException
  {
    return String.join("/", resourceRoot, Iterables.getOnlyElement(listResources(resourceRoot)));
  }

  protected static List<String> listDataFormatResources() throws IOException
  {
    return listResources(DATA_RESOURCE_ROOT)
        .stream()
        .filter(resource -> !SUPERVISOR_SPEC_TEMPLATE_FILE.equals(resource))
        .collect(Collectors.toList());
  }

  /**
   * Returns a map of key to path to spec. The returned map contains at least 2 specs and one of them
   * should be a {@link #SERIALIZER} spec.
   */
  protected static Map<String, String> findTestSpecs(String resourceRoot) throws IOException
  {
    final List<String> specDirs = listResources(resourceRoot);
    final Map<String, String> map = new HashMap<>();
    for (String eachSpec : specDirs) {
      if (SERIALIZER_SPEC_DIR.equals(eachSpec)) {
        map.put(SERIALIZER, getOnlyResourcePath(String.join("/", resourceRoot, SERIALIZER_SPEC_DIR)));
      } else if (INPUT_ROW_PARSER_SPEC_DIR.equals(eachSpec)) {
        map.put(INPUT_ROW_PARSER, getOnlyResourcePath(String.join("/", resourceRoot, INPUT_ROW_PARSER_SPEC_DIR)));
      } else if (INPUT_FORMAT_SPEC_DIR.equals(eachSpec)) {
        map.put(INPUT_FORMAT, getOnlyResourcePath(String.join("/", resourceRoot, INPUT_FORMAT_SPEC_DIR)));
      }
    }
    if (!map.containsKey(SERIALIZER_SPEC_DIR)) {
      throw new IAE("Failed to find serializer spec under [%s]. Found resources are %s", resourceRoot, map);
    }
    if (map.size() == 1) {
      throw new IAE("Failed to find input format or parser spec under [%s]. Found resources are %s", resourceRoot, map);
    }
    return map;
  }

  private Closeable createResourceCloser(GeneratedTestConfig generatedTestConfig)
  {
    return Closer.create().register(() -> doMethodTeardown(generatedTestConfig));
  }

  protected void doTestIndexDataStableState(
      @Nullable Boolean transactionEnabled,
      String serializerPath,
      String parserType,
      String specPath
  ) throws Exception
  {
    final EventSerializer serializer = jsonMapper.readValue(getResourceAsStream(serializerPath), EventSerializer.class);
    final StreamGenerator streamGenerator = new WikipediaStreamEventStreamGenerator(
        serializer,
        EVENTS_PER_SECOND,
        CYCLE_PADDING_MS
    );
    final GeneratedTestConfig generatedTestConfig = new GeneratedTestConfig(parserType, getResourceAsString(specPath));
    try (
        final Closeable closer = createResourceCloser(generatedTestConfig);
        final StreamEventWriter streamEventWriter = createStreamEventWriter(config, transactionEnabled)
    ) {
      final String taskSpec = generatedTestConfig.getStreamIngestionPropsTransform()
                                                 .apply(getResourceAsString(SUPERVISOR_SPEC_TEMPLATE_PATH));
      LOG.info("supervisorSpec: [%s]\n", taskSpec);
      // Start supervisor
      generatedTestConfig.setSupervisorId(indexer.submitSupervisor(taskSpec));
      LOG.info("Submitted supervisor");
      // Start data generator
      streamGenerator.run(
          generatedTestConfig.getStreamName(),
          streamEventWriter,
          TOTAL_NUMBER_OF_SECOND,
          FIRST_EVENT_TIME
      );
      verifyIngestedData(generatedTestConfig);
    }
  }

  void doTestIndexDataWithLosingCoordinator(@Nullable Boolean transactionEnabled) throws Exception
  {
    testIndexWithLosingNodeHelper(
        () -> druidClusterAdminClient.restartCoordinatorContainer(),
        () -> druidClusterAdminClient.waitUntilCoordinatorReady(),
        transactionEnabled
    );
  }

  void doTestIndexDataWithLosingOverlord(@Nullable Boolean transactionEnabled) throws Exception
  {
    testIndexWithLosingNodeHelper(
        () -> druidClusterAdminClient.restartIndexerContainer(),
        () -> druidClusterAdminClient.waitUntilIndexerReady(),
        transactionEnabled
    );
  }

  void doTestIndexDataWithLosingHistorical(@Nullable Boolean transactionEnabled) throws Exception
  {
    testIndexWithLosingNodeHelper(
        () -> druidClusterAdminClient.restartHistoricalContainer(),
        () -> druidClusterAdminClient.waitUntilHistoricalReady(),
        transactionEnabled
    );
  }

  protected void doTestIndexDataWithStartStopSupervisor(@Nullable Boolean transactionEnabled) throws Exception
  {
    final GeneratedTestConfig generatedTestConfig = new GeneratedTestConfig(
        INPUT_FORMAT,
        getResourceAsString(JSON_INPUT_FORMAT_PATH)
    );
    try (
        final Closeable closer = createResourceCloser(generatedTestConfig);
        final StreamEventWriter streamEventWriter = createStreamEventWriter(config, transactionEnabled)
    ) {
      final String taskSpec = generatedTestConfig.getStreamIngestionPropsTransform()
                                                 .apply(getResourceAsString(SUPERVISOR_SPEC_TEMPLATE_PATH));
      LOG.info("supervisorSpec: [%s]\n", taskSpec);
      // Start supervisor
      generatedTestConfig.setSupervisorId(indexer.submitSupervisor(taskSpec));
      LOG.info("Submitted supervisor");
      // Start generating half of the data
      int secondsToGenerateRemaining = TOTAL_NUMBER_OF_SECOND;
      int secondsToGenerateFirstRound = TOTAL_NUMBER_OF_SECOND / 2;
      secondsToGenerateRemaining = secondsToGenerateRemaining - secondsToGenerateFirstRound;
      final StreamGenerator streamGenerator = new WikipediaStreamEventStreamGenerator(
          new JsonEventSerializer(jsonMapper),
          EVENTS_PER_SECOND,
          CYCLE_PADDING_MS
      );
      streamGenerator.run(
          generatedTestConfig.getStreamName(),
          streamEventWriter,
          secondsToGenerateFirstRound,
          FIRST_EVENT_TIME
      );
      // Verify supervisor is healthy before suspension
      ITRetryUtil.retryUntil(
          () -> SupervisorStateManager.BasicState.RUNNING.equals(indexer.getSupervisorStatus(generatedTestConfig.getSupervisorId())),
          true,
          10000,
          30,
          "Waiting for supervisor to be healthy"
      );
      // Suspend the supervisor
      indexer.suspendSupervisor(generatedTestConfig.getSupervisorId());
      // Start generating remainning half of the data
      streamGenerator.run(
          generatedTestConfig.getStreamName(),
          streamEventWriter,
          secondsToGenerateRemaining,
          FIRST_EVENT_TIME.plusSeconds(secondsToGenerateFirstRound)
      );
      // Resume the supervisor
      indexer.resumeSupervisor(generatedTestConfig.getSupervisorId());
      // Verify supervisor is healthy after suspension
      ITRetryUtil.retryUntil(
          () -> SupervisorStateManager.BasicState.RUNNING.equals(indexer.getSupervisorStatus(generatedTestConfig.getSupervisorId())),
          true,
          10000,
          30,
          "Waiting for supervisor to be healthy"
      );
      // Verify that supervisor can catch up with the stream
      verifyIngestedData(generatedTestConfig);
    }
  }

  protected void doTestIndexDataWithStreamReshardSplit(@Nullable Boolean transactionEnabled) throws Exception
  {
    // Reshard the stream from STREAM_SHARD_COUNT to STREAM_SHARD_COUNT * 2
    testIndexWithStreamReshardHelper(transactionEnabled, STREAM_SHARD_COUNT * 2);
  }

  protected void doTestIndexDataWithStreamReshardMerge() throws Exception
  {
    // Reshard the stream from STREAM_SHARD_COUNT to STREAM_SHARD_COUNT / 2
    testIndexWithStreamReshardHelper(null, STREAM_SHARD_COUNT / 2);
  }

  private void testIndexWithLosingNodeHelper(
      Runnable restartRunnable,
      Runnable waitForReadyRunnable,
      @Nullable Boolean transactionEnabled
  ) throws Exception
  {
    final GeneratedTestConfig generatedTestConfig = new GeneratedTestConfig(
        INPUT_FORMAT,
        getResourceAsString(JSON_INPUT_FORMAT_PATH)
    );
    try (
        final Closeable closer = createResourceCloser(generatedTestConfig);
        final StreamEventWriter streamEventWriter = createStreamEventWriter(config, transactionEnabled)
    ) {
      final String taskSpec = generatedTestConfig.getStreamIngestionPropsTransform()
                                                 .apply(getResourceAsString(SUPERVISOR_SPEC_TEMPLATE_PATH));
      LOG.info("supervisorSpec: [%s]\n", taskSpec);
      // Start supervisor
      generatedTestConfig.setSupervisorId(indexer.submitSupervisor(taskSpec));
      LOG.info("Submitted supervisor");
      // Start generating one third of the data (before restarting)
      int secondsToGenerateRemaining = TOTAL_NUMBER_OF_SECOND;
      int secondsToGenerateFirstRound = TOTAL_NUMBER_OF_SECOND / 3;
      secondsToGenerateRemaining = secondsToGenerateRemaining - secondsToGenerateFirstRound;
      final StreamGenerator streamGenerator = new WikipediaStreamEventStreamGenerator(
          new JsonEventSerializer(jsonMapper),
          EVENTS_PER_SECOND,
          CYCLE_PADDING_MS
      );
      streamGenerator.run(
          generatedTestConfig.getStreamName(),
          streamEventWriter,
          secondsToGenerateFirstRound,
          FIRST_EVENT_TIME
      );
      // Verify supervisor is healthy before restart
      ITRetryUtil.retryUntil(
          () -> SupervisorStateManager.BasicState.RUNNING.equals(indexer.getSupervisorStatus(generatedTestConfig.getSupervisorId())),
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
      streamGenerator.run(
          generatedTestConfig.getStreamName(),
          streamEventWriter,
          secondsToGenerateSecondRound,
          FIRST_EVENT_TIME.plusSeconds(secondsToGenerateFirstRound)
      );
      // Wait for Druid process to be available
      LOG.info("Waiting for Druid process to be available");
      waitForReadyRunnable.run();
      LOG.info("Druid process is now available");
      // Start generating remaining data (after restarting)
      streamGenerator.run(
          generatedTestConfig.getStreamName(),
          streamEventWriter,
          secondsToGenerateRemaining,
          FIRST_EVENT_TIME.plusSeconds(secondsToGenerateFirstRound + secondsToGenerateSecondRound)
      );
      // Verify supervisor is healthy
      ITRetryUtil.retryUntil(
          () -> SupervisorStateManager.BasicState.RUNNING.equals(indexer.getSupervisorStatus(generatedTestConfig.getSupervisorId())),
          true,
          10000,
          30,
          "Waiting for supervisor to be healthy"
      );
      // Verify that supervisor ingested all data
      verifyIngestedData(generatedTestConfig);
    }
  }

  private void testIndexWithStreamReshardHelper(@Nullable Boolean transactionEnabled, int newShardCount)
      throws Exception
  {
    final GeneratedTestConfig generatedTestConfig = new GeneratedTestConfig(
        INPUT_FORMAT,
        getResourceAsString(JSON_INPUT_FORMAT_PATH)
    );
    try (
        final Closeable closer = createResourceCloser(generatedTestConfig);
        final StreamEventWriter streamEventWriter = createStreamEventWriter(config, transactionEnabled)
    ) {
      final String taskSpec = generatedTestConfig.getStreamIngestionPropsTransform()
                                                 .apply(getResourceAsString(SUPERVISOR_SPEC_TEMPLATE_PATH));
      LOG.info("supervisorSpec: [%s]\n", taskSpec);
      // Start supervisor
      generatedTestConfig.setSupervisorId(indexer.submitSupervisor(taskSpec));
      LOG.info("Submitted supervisor");
      // Start generating one third of the data (before resharding)
      int secondsToGenerateRemaining = TOTAL_NUMBER_OF_SECOND;
      int secondsToGenerateFirstRound = TOTAL_NUMBER_OF_SECOND / 3;
      secondsToGenerateRemaining = secondsToGenerateRemaining - secondsToGenerateFirstRound;
      final StreamGenerator streamGenerator = new WikipediaStreamEventStreamGenerator(
          new JsonEventSerializer(jsonMapper),
          EVENTS_PER_SECOND,
          CYCLE_PADDING_MS
      );
      streamGenerator.run(
          generatedTestConfig.getStreamName(),
          streamEventWriter,
          secondsToGenerateFirstRound,
          FIRST_EVENT_TIME
      );
      // Verify supervisor is healthy before resahrding
      ITRetryUtil.retryUntil(
          () -> SupervisorStateManager.BasicState.RUNNING.equals(indexer.getSupervisorStatus(generatedTestConfig.getSupervisorId())),
          true,
          10000,
          30,
          "Waiting for supervisor to be healthy"
      );
      // Reshard the supervisor by split from STREAM_SHARD_COUNT to newShardCount and waits until the resharding starts
      streamAdminClient.updatePartitionCount(generatedTestConfig.getStreamName(), newShardCount, true);
      // Start generating one third of the data (while resharding)
      int secondsToGenerateSecondRound = TOTAL_NUMBER_OF_SECOND / 3;
      secondsToGenerateRemaining = secondsToGenerateRemaining - secondsToGenerateSecondRound;
      streamGenerator.run(
          generatedTestConfig.getStreamName(),
          streamEventWriter,
          secondsToGenerateSecondRound,
          FIRST_EVENT_TIME.plusSeconds(secondsToGenerateFirstRound)
      );
      // Wait for stream to finish resharding
      ITRetryUtil.retryUntil(
          () -> streamAdminClient.isStreamActive(generatedTestConfig.getStreamName()),
          true,
          10000,
          30,
          "Waiting for stream to finish resharding"
      );
      ITRetryUtil.retryUntil(
          () -> streamAdminClient.verfiyPartitionCountUpdated(
              generatedTestConfig.getStreamName(),
              STREAM_SHARD_COUNT,
              newShardCount
          ),
          true,
          10000,
          30,
          "Waiting for stream to finish resharding"
      );
      // Start generating remaining data (after resharding)
      streamGenerator.run(
          generatedTestConfig.getStreamName(),
          streamEventWriter,
          secondsToGenerateRemaining,
          FIRST_EVENT_TIME.plusSeconds(secondsToGenerateFirstRound + secondsToGenerateSecondRound)
      );
      // Verify supervisor is healthy after resahrding
      ITRetryUtil.retryUntil(
          () -> SupervisorStateManager.BasicState.RUNNING.equals(indexer.getSupervisorStatus(generatedTestConfig.getSupervisorId())),
          true,
          10000,
          30,
          "Waiting for supervisor to be healthy"
      );
      // Verify that supervisor can catch up with the stream
      verifyIngestedData(generatedTestConfig);
    }
  }

  private void verifyIngestedData(GeneratedTestConfig generatedTestConfig) throws Exception
  {
    // Wait for supervisor to consume events
    LOG.info("Waiting for [%s] millis for stream indexing tasks to consume events", WAIT_TIME_MILLIS);
    Thread.sleep(WAIT_TIME_MILLIS);
    // Query data
    final String querySpec = generatedTestConfig.getStreamQueryPropsTransform()
                                                .apply(getResourceAsString(QUERIES_FILE));
    // this query will probably be answered from the indexing tasks but possibly from 2 historical segments / 2 indexing
    this.queryHelper.testQueriesFromString(querySpec, 2);
    LOG.info("Shutting down supervisor");
    indexer.shutdownSupervisor(generatedTestConfig.getSupervisorId());
    // Clear supervisor ID to not shutdown again.
    generatedTestConfig.setSupervisorId(null);
    // wait for all indexing tasks to finish
    LOG.info("Waiting for all indexing tasks to finish");
    ITRetryUtil.retryUntilTrue(
        () -> (indexer.getUncompletedTasksForDataSource(generatedTestConfig.getFullDatasourceName()).size() == 0),
        "Waiting for Tasks Completion"
    );
    // wait for segments to be handed off
    ITRetryUtil.retryUntil(
        () -> coordinator.areSegmentsLoaded(generatedTestConfig.getFullDatasourceName()),
        true,
        10000,
        30,
        "Real-time generated segments loaded"
    );

    // this query will be answered by at least 1 historical segment, most likely 2, and possibly up to all 4
    this.queryHelper.testQueriesFromString(querySpec, 2);
  }

  long getSumOfEventSequence(int numEvents)
  {
    return (numEvents * (1 + numEvents)) / 2;
  }

  private void doMethodTeardown(GeneratedTestConfig generatedTestConfig)
  {
    if (generatedTestConfig.getSupervisorId() != null) {
      try {
        indexer.shutdownSupervisor(generatedTestConfig.getSupervisorId());
      }
      catch (Exception e) {
        // Best effort cleanup as the supervisor may have already been cleanup
        LOG.warn(e, "Failed to cleanup supervisor. This might be expected depending on the test method");
      }
    }
    try {
      unloader(generatedTestConfig.getFullDatasourceName());
    }
    catch (Exception e) {
      // Best effort cleanup as the datasource may have already been cleanup
      LOG.warn(e, "Failed to cleanup datasource. This might be expected depending on the test method");
    }
    try {
      streamAdminClient.deleteStream(generatedTestConfig.getStreamName());
    }
    catch (Exception e) {
      // Best effort cleanup as the stream may have already been cleanup
      LOG.warn(e, "Failed to cleanup stream. This might be expected depending on the test method");
    }
  }

  private class GeneratedTestConfig
  {
    private final String streamName;
    private final String fullDatasourceName;
    private String supervisorId;
    private Function<String, String> streamIngestionPropsTransform;
    private Function<String, String> streamQueryPropsTransform;

    GeneratedTestConfig(String parserType, String parserOrInputFormat) throws Exception
    {
      streamName = getTestNamePrefix() + "_index_test_" + UUID.randomUUID();
      String datasource = getTestNamePrefix() + "_indexing_service_test_" + UUID.randomUUID();
      Map<String, String> tags = ImmutableMap.of(
          STREAM_EXPIRE_TAG,
          Long.toString(DateTimes.nowUtc().plusMinutes(30).getMillis())
      );
      streamAdminClient.createStream(streamName, STREAM_SHARD_COUNT, tags);
      ITRetryUtil.retryUntil(
          () -> streamAdminClient.isStreamActive(streamName),
          true,
          10000,
          30,
          "Wait for stream active"
      );
      fullDatasourceName = datasource + config.getExtraDatasourceNameSuffix();
      streamIngestionPropsTransform = generateStreamIngestionPropsTransform(
          streamName,
          fullDatasourceName,
          parserType,
          parserOrInputFormat,
          config
      );
      streamQueryPropsTransform = generateStreamQueryPropsTransform(streamName, fullDatasourceName);
    }

    public String getSupervisorId()
    {
      return supervisorId;
    }

    public void setSupervisorId(String supervisorId)
    {
      this.supervisorId = supervisorId;
    }

    public String getStreamName()
    {
      return streamName;
    }

    public String getFullDatasourceName()
    {
      return fullDatasourceName;
    }

    public Function<String, String> getStreamIngestionPropsTransform()
    {
      return streamIngestionPropsTransform;
    }

    public Function<String, String> getStreamQueryPropsTransform()
    {
      return streamQueryPropsTransform;
    }
  }
}
