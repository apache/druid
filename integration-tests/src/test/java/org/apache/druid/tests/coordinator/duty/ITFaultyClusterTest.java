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

package org.apache.druid.tests.coordinator.duty;

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.cluster.ClusterTestingTaskConfig;
import org.apache.druid.testing.cluster.overlord.FaultyLagAggregator;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.EventSerializer;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.testing.utils.KafkaUtil;
import org.apache.druid.testing.utils.StreamEventWriter;
import org.apache.druid.testing.utils.StreamGenerator;
import org.apache.druid.testing.utils.WikipediaStreamEventStreamGenerator;
import org.apache.druid.tests.TestNGGroup;
import org.apache.druid.tests.indexer.AbstractKafkaIndexingServiceTest;
import org.apache.druid.tests.indexer.AbstractStreamIndexingTest;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.util.Map;

/**
 * Integration test to verify induction of various faults in the cluster
 * using {@link ClusterTestingTaskConfig}.
 * <p>
 * Future tests can try to leverage the cluster testing config to write tests
 * for cluster scalability and stability.
 */
@Test(groups = {TestNGGroup.KAFKA_INDEX_SLOW})
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITFaultyClusterTest extends AbstractKafkaIndexingServiceTest
{
  private static final Logger log = new Logger(ITFaultyClusterTest.class);

  private GeneratedTestConfig generatedTestConfig;
  private StreamGenerator streamGenerator;

  private String fullDatasourceName;

  @DataProvider
  public static Object[] getParameters()
  {
    return new Object[]{false, true};
  }

  @BeforeClass
  public void setupClass() throws Exception
  {
    doBeforeClass();
  }

  @BeforeMethod
  public void setup() throws Exception
  {
    generatedTestConfig = new GeneratedTestConfig(
        Specs.PARSER_TYPE,
        getResourceAsString(Specs.INPUT_FORMAT_PATH)
    );
    fullDatasourceName = generatedTestConfig.getFullDatasourceName();
    final EventSerializer serializer = jsonMapper.readValue(
        getResourceAsStream(Specs.SERIALIZER_PATH),
        EventSerializer.class
    );
    streamGenerator = new WikipediaStreamEventStreamGenerator(serializer, 6, 100);
  }

  @Override
  public String getTestNamePrefix()
  {
    return "faulty_cluster";
  }

  @Test(dataProvider = "getParameters")
  public void test_streamingIngestion_worksWithFaultyCluster(boolean transactionEnabled) throws Exception
  {
    if (shouldSkipTest(transactionEnabled)) {
      return;
    }

    try (
        final Closeable closer = createResourceCloser(generatedTestConfig);
        final StreamEventWriter streamEventWriter = createStreamEventWriter(config, transactionEnabled)
    ) {
      // Set context parameters
      final Map<String, Object> taskContext = Map.of(
          "clusterTesting",
          Map.of(
              "metadataConfig", Map.of("cleanupPendingSegments", false),
              "coordinatorClientConfig", Map.of("minSegmentHandoffDelay", "PT10S"),
              "taskActionClientConfig", Map.of("segmentAllocateDelay", "PT10S", "segmentPublishDelay", "PT10S")
          )
      );

      // Start supervisor
      final String supervisorSpec = generatedTestConfig
          .withContext(taskContext)
          .withLagAggregator(new FaultyLagAggregator(1_000_000))
          .getStreamIngestionPropsTransform()
          .apply(getResourceAsString(SUPERVISOR_SPEC_TEMPLATE_PATH));

      final String supervisorId = indexer.submitSupervisor(supervisorSpec);
      generatedTestConfig.setSupervisorId(supervisorId);
      log.info("Submitted supervisor[%s] with spec[%s]", supervisorId, supervisorSpec);

      // Generate data for minutes 1, 2 and 3
      final DateTime firstRecordTime = DateTimes.of("2000-01-01T01:01:00Z");
      final long rowsForMinute1 = generateDataForMinute(firstRecordTime, streamEventWriter);
      final long rowsForMinute2 = generateDataForMinute(firstRecordTime.plus(Period.minutes(1)), streamEventWriter);
      final long rowsForMinute3 = generateDataForMinute(firstRecordTime.plus(Period.minutes(2)), streamEventWriter);

      ITRetryUtil.retryUntilTrue(
          () -> {
            final Integer aggregateLag = (Integer) indexer.getFullSupervisorStatus(supervisorId).get("aggregateLag");
            log.info("Aggregate lag is [%d].", aggregateLag);
            return aggregateLag != null && aggregateLag > 1_000_000;
          },
          "Aggregate lag exceeds 1M"
      );

      // Wait for data to be ingested for all the minutes
      waitUntilDatasourceRowCountEquals(fullDatasourceName, rowsForMinute1 + rowsForMinute2 + rowsForMinute3);
      waitForSegmentsToLoad(fullDatasourceName);

      // 2 segments for each minute, total 6
      waitUntilDatasourceSegmentCountEquals(fullDatasourceName, 6);
    }
  }

  /**
   * Generates data points for a minute with the specified start time.
   *
   * @return Number of rows generated.
   */
  private long generateDataForMinute(DateTime firstRecordTime, StreamEventWriter streamEventWriter)
  {
    final long rowCount = streamGenerator.run(
        generatedTestConfig.getStreamName(),
        streamEventWriter,
        10,
        firstRecordTime
    );
    log.info("Generated [%d] rows for 1 minute interval with start[%s]", rowCount, firstRecordTime);

    return rowCount;
  }

  /**
   * Checks if a test should be skipped based on whether transaction is enabled or not.
   */
  private boolean shouldSkipTest(boolean testEnableTransaction)
  {
    Map<String, String> kafkaTestProps = KafkaUtil
        .getAdditionalKafkaTestConfigFromProperties(config);
    boolean configEnableTransaction = Boolean.parseBoolean(
        kafkaTestProps.getOrDefault(KafkaUtil.TEST_CONFIG_TRANSACTION_ENABLED, "false")
    );

    return configEnableTransaction != testEnableTransaction;
  }

  /**
   * Constants for test specs.
   */
  private static class Specs
  {
    static final String SERIALIZER_PATH = DATA_RESOURCE_ROOT + "/csv/serializer/serializer.json";
    static final String INPUT_FORMAT_PATH = DATA_RESOURCE_ROOT + "/csv/input_format/input_format.json";
    static final String PARSER_TYPE = AbstractStreamIndexingTest.INPUT_FORMAT;
  }

}
