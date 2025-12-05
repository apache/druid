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

package org.apache.druid.testing.embedded.query;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.indexing.MoreResources;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.druid.testing.embedded.query.ServerManagerForQueryErrorTest.QUERY_RETRY_TEST_CONTEXT_KEY;
import static org.apache.druid.testing.embedded.query.ServerManagerForQueryErrorTest.QUERY_RETRY_UNAVAILABLE_SEGMENT_IDX_KEY;

/**
 * This class tests the query retry on missing segments. A segment can be missing in a historical during a query if
 * the historical drops the segment after the broker issues the query to the historical. To mimic this case, this
 * test spawns a historical modified for testing. This historical announces all segments assigned, but doesn't serve
 * all of them always. Instead, it can report missing segments for some segments.
 */
public class QueryRetryOnMissingSegmentsTest extends QueryTestBase
{
  /**
   * This enumeration represents an expectation after finishing running the test query.
   */
  private enum Expectation
  {
    /**
     * Expect that the test for a query succeeds and with correct results.
     */
    ALL_SUCCESS,
    /**
     * Expect that the test query returns the 200 HTTP response, but will surely return incorrect result.
     */
    INCORRECT_RESULT,
    /**
     * Expect that the test query must return the 500 HTTP response.
     */
    QUERY_FAILURE
  }

  private ObjectMapper jsonMapper;
  private String tableName;

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    overlord.addProperty("druid.manager.segments.pollDuration", "PT0.1s");
    coordinator.addProperty("druid.manager.segments.useIncrementalCache", "always");
    indexer.setServerMemory(400_000_000)
           .addProperty("druid.worker.capacity", "4")
           .addProperty("druid.processing.numThreads", "2")
           .addProperty("druid.segment.handoff.pollDuration", "PT0.1s");

    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addServer(overlord)
                               .addServer(coordinator)
                               .addServer(broker)
                               .addServer(router)
                               .addServer(indexer)
                               .addServer(historical)
                               .addExtension(ServerManagerForQueryErrorTestModule.class);
  }

  @Override
  public void beforeAll()
  {
    jsonMapper = overlord.bindings().jsonMapper();
    tableName = EmbeddedClusterApis.createTestDatasourceName(getDatasourcePrefix());

    final String taskId = IdUtils.getRandomId();
    final IndexTask task = MoreResources.Task.BASIC_INDEX.get().dataSource(tableName).withId(taskId);
    cluster.callApi().onLeaderOverlord(o -> o.runTask(taskId, task));
    cluster.callApi().waitForTaskToSucceed(taskId, overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(tableName, coordinator, broker);
  }

  @Test
  public void testWithRetriesDisabledPartialResultDisallowed()
  {
    // Since retry is disabled and a partial result is not allowed, the query must fail.
    test(buildQuery(0, false, -1), Expectation.QUERY_FAILURE);
  }

  @Test
  public void testWithRetriesDisabledPartialResultAllowed()
  {
    // Since retry is disabled but a partial result is allowed, the query must succeed.
    // However, the query must return an incorrect result.
    test(buildQuery(0, true, -1), Expectation.INCORRECT_RESULT);
  }

  @Test
  public void testWithRetriesEnabledPartialResultDisallowed()
  {
    // Since retry is enabled, the query must succeed even though a partial result is disallowed.
    // The retry count is set to 1 since on the first retry of the query (i.e. second overall try), the historical
    // will start processing the segment and not call it missing.
    // The query must return correct results.
    test(buildQuery(1, false, -1), Expectation.ALL_SUCCESS);
  }

  @Test
  public void testFailureWhenLastSegmentIsMissingWithPartialResultsDisallowed()
  {
    // Since retry is disabled and a partial result is not allowed, the query must fail since the last segment
    // is missing/unavailable.
    test(buildQuery(0, false, 2), Expectation.QUERY_FAILURE);
  }

  private void test(ClientSqlQuery clientSqlQuery, Expectation expectation)
  {
    int querySuccess = 0;
    int queryFailure = 0;
    int resultMatches = 0;
    int resultMismatches = 0;

    String resultAsJson;
    try {
      resultAsJson = cluster.callApi().onAnyBroker(b -> b.submitSqlQuery(clientSqlQuery));
      querySuccess++;
    }
    catch (Exception e) {
      queryFailure++;
      resultAsJson = e.getMessage();
    }

    if (querySuccess > 0) {
      List<Map<String, Object>> result = JacksonUtils.readValue(
          jsonMapper,
          resultAsJson.getBytes(StandardCharsets.UTF_8),
          new TypeReference<>()
          {
          }
      );

      if (expectation == Expectation.ALL_SUCCESS) {
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(10, result.get(0).get("cnt"));
        resultMatches++;
      } else if (expectation == Expectation.INCORRECT_RESULT) {
        // When the result is expected to be incorrect, we just check that the count is not the expected value.
        Assertions.assertEquals(1, result.size());
        Assertions.assertNotEquals(10, result.get(0).get("cnt"));
        resultMismatches++;
      }
    }

    switch (expectation) {
      case ALL_SUCCESS:
        Assertions.assertEquals(1, querySuccess);
        Assertions.assertEquals(0, queryFailure);
        Assertions.assertEquals(1, resultMatches);
        Assertions.assertEquals(0, resultMismatches);
        break;
      case INCORRECT_RESULT:
        Assertions.assertEquals(1, querySuccess);
        Assertions.assertEquals(0, queryFailure);
        Assertions.assertEquals(0, resultMatches);
        Assertions.assertEquals(1, resultMismatches);
        break;
      case QUERY_FAILURE:
        Assertions.assertEquals(0, querySuccess);
        Assertions.assertEquals(1, queryFailure);
        Assertions.assertEquals(0, resultMatches);
        Assertions.assertEquals(0, resultMismatches);
        break;
      default:
        throw new ISE("Unknown expectation[%s]", expectation);
    }
  }

  private ClientSqlQuery buildQuery(
      int numRetriesOnMissingSegments,
      boolean allowPartialResults,
      int unavailableSegmentIdx
  )
  {
    return new ClientSqlQuery(
        StringUtils.format("SELECT count(item) as cnt FROM %s", tableName),
        null,
        false,
        false,
        false,
        buildContext(
            numRetriesOnMissingSegments,
            allowPartialResults,
            unavailableSegmentIdx
        ),
        List.of()
    );
  }

  private static Map<String, Object> buildContext(
      int numRetriesOnMissingSegments,
      boolean allowPartialResults,
      int unavailableSegmentIdx
  )
  {
    final Map<String, Object> context = new HashMap<>();
    // Disable cache so that each run hits historical.
    context.put(QueryContexts.USE_CACHE_KEY, false);
    context.put(QueryContexts.USE_RESULT_LEVEL_CACHE_KEY, false);
    context.put(QueryContexts.NUM_RETRIES_ON_MISSING_SEGMENTS_KEY, numRetriesOnMissingSegments);
    context.put(QueryContexts.RETURN_PARTIAL_RESULTS_KEY, allowPartialResults);
    context.put(QUERY_RETRY_TEST_CONTEXT_KEY, true);
    context.put(QUERY_RETRY_UNAVAILABLE_SEGMENT_IDX_KEY, unavailableSegmentIdx);
    return context;
  }
}
