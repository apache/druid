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

package org.apache.druid.tests.query;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.server.coordination.ServerManagerForQueryRetryTest;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.clients.CoordinatorResourceTestClient;
import org.apache.druid.testing.clients.QueryResourceTestClient;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.testing.utils.QueryResultVerifier;
import org.apache.druid.testing.utils.QueryWithResults;
import org.apache.druid.testing.utils.TestQueryHelper;
import org.apache.druid.tests.TestNGGroup;
import org.apache.druid.tests.indexer.AbstractIndexerTest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class tests the query retry on missing segments. A segment can be missing in a historical during a query if
 * the historical drops the segment after the broker issues the query to the historical. To mimic this case, this
 * test spawns two historicals, a normal historical and a historical modified for testing. The later historical
 * announces all segments assigned, but doesn't serve all of them. Instead, it can report missing segments for some
 * segments. See {@link ServerManagerForQueryRetryTest} for more details.
 *
 * To run this test properly, the test group must be specified as {@link TestNGGroup#QUERY_RETRY}.
 */
@Test(groups = TestNGGroup.QUERY_RETRY)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITQueryRetryTestOnMissingSegments
{
  private static final String WIKIPEDIA_DATA_SOURCE = "wikipedia_editstream";
  private static final String QUERIES_RESOURCE = "/queries/wikipedia_editstream_queries_query_retry_test.json";
  private static final int TIMES_TO_RUN = 50;

  /**
   * This test runs the same query multiple times. This enumeration represents an expectation after finishing
   * running the query.
   */
  private enum Expectation
  {
    /**
     * Expect that all runs succeed.
     */
    ALL_SUCCESS,
    /**
     * Expect that all runs returns the 200 HTTP response, but some of them can return incorrect result.
     */
    INCORRECT_RESULT,
    /**
     * Expect that some runs can return the 500 HTTP response. For the runs returned the 200 HTTP response, the query
     * result must be correct.
     */
    QUERY_FAILURE
  }

  @Inject
  private CoordinatorResourceTestClient coordinatorClient;
  @Inject
  private TestQueryHelper queryHelper;
  @Inject
  private QueryResourceTestClient queryClient;
  @Inject
  private IntegrationTestingConfig config;
  @Inject
  private ObjectMapper jsonMapper;

  @BeforeMethod
  public void before()
  {
    // ensure that wikipedia segments are loaded completely
    ITRetryUtil.retryUntilTrue(
        () -> coordinatorClient.areSegmentsLoaded(WIKIPEDIA_DATA_SOURCE), "wikipedia segment load"
    );
  }

  @Test
  public void testWithRetriesDisabledPartialResultDisallowed() throws Exception
  {
    // Since retry is disabled and partial result is not allowed, we can expect some queries can fail.
    // If a query succeed, its result must be correct.
    testQueries(buildQuery(0, false), Expectation.QUERY_FAILURE);
  }

  @Test
  public void testWithRetriesDisabledPartialResultAllowed() throws Exception
  {
    // Since retry is disabled but partial result is allowed, all queries must succeed.
    // However, some queries can return incorrect result.
    testQueries(buildQuery(0, true), Expectation.INCORRECT_RESULT);
  }

  @Test
  public void testWithRetriesEnabledPartialResultDisallowed() throws Exception
  {
    // Since retry is enabled, all queries must succeed even though partial result is disallowed.
    // All queries must return correct result.
    testQueries(buildQuery(30, false), Expectation.ALL_SUCCESS);
  }

  private void testQueries(String queryWithResultsStr, Expectation expectation) throws Exception
  {
    final List<QueryWithResults> queries = jsonMapper.readValue(
        queryWithResultsStr,
        new TypeReference<List<QueryWithResults>>() {}
    );
    testQueries(queries, expectation);
  }

  private void testQueries(List<QueryWithResults> queries, Expectation expectation) throws Exception
  {
    int querySuccess = 0;
    int queryFailure = 0;
    int resultMatches = 0;
    int resultMismatches = 0;
    for (int i = 0; i < TIMES_TO_RUN; i++) {
      for (QueryWithResults queryWithResult : queries) {
        final StatusResponseHolder responseHolder = queryClient
            .queryAsync(queryHelper.getQueryURL(config.getBrokerUrl()), queryWithResult.getQuery())
            .get();

        if (responseHolder.getStatus().getCode() == HttpResponseStatus.OK.getCode()) {
          querySuccess++;

          List<Map<String, Object>> result = jsonMapper.readValue(
              responseHolder.getContent(),
              new TypeReference<List<Map<String, Object>>>() {}
          );
          if (!QueryResultVerifier.compareResults(result, queryWithResult.getExpectedResults())) {
            if (expectation != Expectation.INCORRECT_RESULT) {
              throw new ISE(
                  "Incorrect query results for query %s \n expectedResults: %s \n actualResults : %s",
                  queryWithResult.getQuery(),
                  jsonMapper.writeValueAsString(queryWithResult.getExpectedResults()),
                  jsonMapper.writeValueAsString(result)
              );
            } else {
              resultMismatches++;
            }
          } else {
            resultMatches++;
          }
        } else if (responseHolder.getStatus().getCode() == HttpResponseStatus.INTERNAL_SERVER_ERROR.getCode() &&
                   expectation == Expectation.QUERY_FAILURE) {
          final Map<String, Object> response = jsonMapper.readValue(responseHolder.getContent(), Map.class);
          final String errorMessage = (String) response.get("errorMessage");
          Assert.assertNotNull(errorMessage, "errorMessage");
          Assert.assertTrue(errorMessage.contains("No results found for segments"));
          queryFailure++;
        } else {
          throw new ISE(
              "Unexpected failure, code: [%s], content: [%s]",
              responseHolder.getStatus(),
              responseHolder.getContent()
          );
        }
      }
    }

    switch (expectation) {
      case ALL_SUCCESS:
        Assert.assertEquals(querySuccess, ITQueryRetryTestOnMissingSegments.TIMES_TO_RUN);
        Assert.assertEquals(queryFailure, 0);
        Assert.assertEquals(resultMatches, ITQueryRetryTestOnMissingSegments.TIMES_TO_RUN);
        Assert.assertEquals(resultMismatches, 0);
        break;
      case QUERY_FAILURE:
        Assert.assertTrue(querySuccess > 0, "At least one query is expected to succeed.");
        Assert.assertTrue(queryFailure > 0, "At least one query is expected to fail.");
        Assert.assertEquals(querySuccess, resultMatches);
        Assert.assertEquals(resultMismatches, 0);
        break;
      case INCORRECT_RESULT:
        Assert.assertEquals(querySuccess, ITQueryRetryTestOnMissingSegments.TIMES_TO_RUN);
        Assert.assertEquals(queryFailure, 0);
        Assert.assertTrue(resultMatches > 0, "At least one query is expected to return correct results.");
        Assert.assertTrue(resultMismatches > 0, "At least one query is expected to return less results.");
        break;
      default:
        throw new ISE("Unknown expectation[%s]", expectation);
    }
  }

  private String buildQuery(int numRetriesOnMissingSegments, boolean allowPartialResults) throws IOException
  {
    return StringUtils.replace(
        AbstractIndexerTest.getResourceAsString(QUERIES_RESOURCE),
        "%%CONTEXT%%",
        jsonMapper.writeValueAsString(buildContext(numRetriesOnMissingSegments, allowPartialResults))
    );
  }

  private static Map<String, Object> buildContext(int numRetriesOnMissingSegments, boolean allowPartialResults)
  {
    final Map<String, Object> context = new HashMap<>();
    // Disable cache so that each run hits historical.
    context.put(QueryContexts.USE_CACHE_KEY, false);
    context.put(QueryContexts.NUM_RETRIES_ON_MISSING_SEGMENTS_KEY, numRetriesOnMissingSegments);
    context.put(QueryContexts.RETURN_PARTIAL_RESULTS_KEY, allowPartialResults);
    context.put(ServerManagerForQueryRetryTest.QUERY_RETRY_TEST_CONTEXT_KEY, true);
    return context;
  }
}
