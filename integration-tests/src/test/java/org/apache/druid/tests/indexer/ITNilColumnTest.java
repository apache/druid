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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManager;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.testing.clients.TaskResponseObject;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.testing.utils.JsonEventSerializer;
import org.apache.druid.testing.utils.SqlQueryWithResults;
import org.apache.druid.testing.utils.StreamEventWriter;
import org.apache.druid.testing.utils.StreamGenerator;
import org.apache.druid.testing.utils.WikipediaStreamEventStreamGenerator;
import org.apache.druid.tests.TestNGGroup;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Test(groups = TestNGGroup.TRANSACTIONAL_KAFKA_INDEX_SLOW)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITNilColumnTest extends AbstractKafkaIndexingServiceTest
{
  private static final Logger LOG = new Logger(ITNilColumnTest.class);

  @Override
  public String getTestNamePrefix()
  {
    return "nil-column-test";
  }

  @BeforeClass
  public void beforeClass() throws Exception
  {
    doBeforeClass();
  }

  @Test
  public void testQueryNilColumnBeforeAndAfterPublishingSegments() throws Exception
  {
    final List<String> dimensions = new ArrayList<>(DEFAULT_DIMENSIONS.size() + 2);
    dimensions.add("nilDim1");
    dimensions.addAll(DEFAULT_DIMENSIONS);
    dimensions.add("nilDim2");
    final GeneratedTestConfig generatedTestConfig = new GeneratedTestConfig(
        INPUT_FORMAT,
        getResourceAsString(JSON_INPUT_FORMAT_PATH),
        dimensions
    );
    try (
        final Closeable closer = createResourceCloser(generatedTestConfig);
        final StreamEventWriter streamEventWriter = createStreamEventWriter(config, true)
    ) {
      final String taskSpec = generatedTestConfig.getStreamIngestionPropsTransform()
                                                 .apply(getResourceAsString(SUPERVISOR_SPEC_TEMPLATE_PATH));
      LOG.info("supervisorSpec: [%s]\n", taskSpec);
      // Start supervisor
      generatedTestConfig.setSupervisorId(indexer.submitSupervisor(taskSpec));
      LOG.info("Submitted supervisor");
      // Start generating half of the data
      final StreamGenerator streamGenerator = new WikipediaStreamEventStreamGenerator(
          new JsonEventSerializer(jsonMapper),
          EVENTS_PER_SECOND,
          CYCLE_PADDING_MS
      );
      long numWritten = streamGenerator.run(
          generatedTestConfig.getStreamName(),
          streamEventWriter,
          TOTAL_NUMBER_OF_SECOND,
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
      // 60 events should have been ingested as per EVENTS_PER_SECOND and TOTAL_NUMBER_OF_SECOND.
      // Since maxRowsInMemory is set to 500,000, every row should be in incrementalIndex.
      // So, let's test if SQL finds nil dimensions from incrementalIndexes.
      dataLoaderHelper.waitUntilDatasourceIsReady(generatedTestConfig.getFullDatasourceName());
      final SqlQueryWithResults queryWithResults = new SqlQueryWithResults(
          new SqlQuery(
              StringUtils.format(
                  "SELECT nilDim1, nilDim2 FROM \"%s\" LIMIT 1",
                  generatedTestConfig.getFullDatasourceName()
              ),
              null,
              false,
              false,
              false,
              null,
              null
          ),
          ImmutableList.of(
              Maps.asMap(ImmutableSet.of("nilDim1", "nilDim2"), k -> NullHandling.defaultStringValue())
          )
      );
      sqlQueryHelper.testQueriesFromString(jsonMapper.writeValueAsString(ImmutableList.of(queryWithResults)));

      // Suspend the supervisor
      indexer.terminateSupervisor(generatedTestConfig.getSupervisorId());
      ITRetryUtil.retryUntilTrue(
          () -> {
            List<TaskResponseObject> tasks = indexer
                .getRunningTasks()
                .stream()
                .filter(task -> task.getId().contains(generatedTestConfig.getFullDatasourceName()))
                .filter(task -> "index_kafka".equals(task.getType()))
                .collect(Collectors.toList());
            LOG.info("[%s] tasks are running", tasks.stream().map(task -> {
              try {
                return jsonMapper.writeValueAsString(task);
              }
              catch (JsonProcessingException e) {
                throw new RuntimeException(e);
              }
            }).collect(Collectors.toList()));
            return tasks.isEmpty();
          },
          "Waiting for all tasks to stop"
      );

      // Now, we should have published all segments.
      // Let's test if SQL finds nil dimensions from queryableIndexes.
      dataLoaderHelper.waitUntilDatasourceIsReady(generatedTestConfig.getFullDatasourceName());
      verifyIngestedData(generatedTestConfig, numWritten);

      sqlQueryHelper.testQueriesFromString(jsonMapper.writeValueAsString(ImmutableList.of(queryWithResults)));
    }
  }
}
