/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.testing.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;

import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.testing.IntegrationTestingConfig;
import io.druid.testing.clients.QueryResourceTestClient;

import java.util.List;
import java.util.Map;

public class TestQueryHelper
{
  public static Logger LOG = new Logger(TestQueryHelper.class);
  private final QueryResourceTestClient queryClient;
  private final ObjectMapper jsonMapper;
  private final String broker;

  @Inject
  TestQueryHelper(
      ObjectMapper jsonMapper,
      QueryResourceTestClient queryClient,
      IntegrationTestingConfig config
  )
  {
    this.jsonMapper = jsonMapper;
    this.queryClient = queryClient;
    this.broker = config.getBrokerHost();
  }

  public void testQueriesFromFile(String filePath, int timesToRun) throws Exception
  {
    testQueriesFromFile(getBrokerURL(), filePath, timesToRun);
  }

  public void testQueriesFromFile(String url, String filePath, int timesToRun) throws Exception
  {
    LOG.info("Starting query tests for [%s]", filePath);
    List<QueryWithResults> queries =
        jsonMapper.readValue(
            TestQueryHelper.class.getResourceAsStream(filePath),
            new TypeReference<List<QueryWithResults>>()
            {
            }
        );
    testQueries(url, queries, timesToRun);
  }

  public void testQueriesFromString(String str, int timesToRun) throws Exception
  {
    testQueriesFromString(getBrokerURL(), str, timesToRun);
  }

  public void testQueriesFromString(String url, String str, int timesToRun) throws Exception
  {
    LOG.info("Starting query tests using\n%s", str);
    List<QueryWithResults> queries =
        jsonMapper.readValue(
            str,
            new TypeReference<List<QueryWithResults>>()
            {
            }
        );
    testQueries(url, queries, timesToRun);
  }

  private void testQueries(String url, List<QueryWithResults> queries, int timesToRun) throws Exception
  {
    for (int i = 0; i < timesToRun; i++) {
      LOG.info("Starting Iteration %d", i);

      boolean failed = false;
      for (QueryWithResults queryWithResult : queries) {
        LOG.info("Running Query %s", queryWithResult.getQuery().getType());
        List<Map<String, Object>> result = queryClient.query(url, queryWithResult.getQuery());
        if (!QueryResultVerifier.compareResults(result, queryWithResult.getExpectedResults())) {
          LOG.error(
              "Failed while executing query %s \n expectedResults: %s \n actualResults : %s",
              queryWithResult.getQuery(),
              jsonMapper.writeValueAsString(queryWithResult.getExpectedResults()),
              jsonMapper.writeValueAsString(result)
          );
          failed = true;
        } else {
          LOG.info("Results Verified for Query %s", queryWithResult.getQuery().getType());
        }
      }

      if (failed) {
        throw new ISE("one or more queries failed");
      }
    }
  }

  private String getBrokerURL()
  {
    return String.format("http://%s/druid/v2?pretty", broker);
  }
}
