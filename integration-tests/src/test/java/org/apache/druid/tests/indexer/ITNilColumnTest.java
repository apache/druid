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
import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManager.BasicState;
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
  private static final String NIL_DIM1 = "nilDim1";
  private static final String NIL_DIM2 = "nilDim2";

  private final List<String> dimensions;

  public ITNilColumnTest()
  {
    this.dimensions = new ArrayList<>(DEFAULT_DIMENSIONS.size() + 2);
    dimensions.add(NIL_DIM1);
    dimensions.addAll(DEFAULT_DIMENSIONS);
    dimensions.add(NIL_DIM2);
  }

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
          () -> BasicState.RUNNING.equals(indexer.getSupervisorStatus(generatedTestConfig.getSupervisorId())),
          true,
          10000,
          30,
          "Waiting for supervisor to be healthy"
      );
      // 60 events should have been ingested as per EVENTS_PER_SECOND and TOTAL_NUMBER_OF_SECOND.
      // Since maxRowsInMemory is set to 500,000, every row should be in incrementalIndex.
      // So, let's test if SQL finds nil dimensions from incrementalIndexes.
      dataLoaderHelper.waitUntilDatasourceIsReady(generatedTestConfig.getFullDatasourceName());
      final List<SqlQueryWithResults> queryWithResults = getQueryWithResults(generatedTestConfig);
      sqlQueryHelper.testQueriesFromString(jsonMapper.writeValueAsString(queryWithResults));
      final List<SqlQueryWithResults> metadataQueryWithResults = getMetadataQueryWithResults(generatedTestConfig);
      sqlQueryHelper.testQueriesFromString(jsonMapper.writeValueAsString(metadataQueryWithResults));

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

      sqlQueryHelper.testQueriesFromString(jsonMapper.writeValueAsString(queryWithResults));
      sqlQueryHelper.testQueriesFromString(jsonMapper.writeValueAsString(metadataQueryWithResults));
    }
  }

  private static List<SqlQueryWithResults> getQueryWithResults(GeneratedTestConfig generatedTestConfig)
  {
    return ImmutableList.of(
        new SqlQueryWithResults(
            new SqlQuery(
                StringUtils.format(
                    "SELECT count(*) FROM \"%s\" WHERE %s IS NOT NULL OR %s IS NOT NULL",
                    generatedTestConfig.getFullDatasourceName(),
                    NIL_DIM1,
                    NIL_DIM2
                ),
                null,
                false,
                false,
                false,
                null,
                null
            ),
            ImmutableList.of(ImmutableMap.of("EXPR$0", 0))
        )
    );
  }

  private List<SqlQueryWithResults> getMetadataQueryWithResults(GeneratedTestConfig generatedTestConfig)
  {
    return ImmutableList.of(
        new SqlQueryWithResults(
            new SqlQuery(
                StringUtils.format(
                    "SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE"
                    + " FROM INFORMATION_SCHEMA.COLUMNS"
                    + " WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN ('%s', '%s')",
                    generatedTestConfig.getFullDatasourceName(),
                    NIL_DIM1,
                    NIL_DIM2
                ),
                null,
                false,
                false,
                false,
                null,
                null
            ),
            ImmutableList.of(
                ImmutableMap.of(
                    "COLUMN_NAME",
                    NIL_DIM1,
                    "IS_NULLABLE",
                    "YES",
                    "DATA_TYPE",
                    "VARCHAR"
                ),
                ImmutableMap.of(
                    "COLUMN_NAME",
                    NIL_DIM2,
                    "IS_NULLABLE",
                    "YES",
                    "DATA_TYPE",
                    "VARCHAR"
                )
            )
        )
    );
  }
}
