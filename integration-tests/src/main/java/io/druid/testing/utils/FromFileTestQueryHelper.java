/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.testing.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import io.druid.testing.clients.QueryResourceTestClient;

import java.util.List;
import java.util.Map;

public class FromFileTestQueryHelper
{
  public static Logger LOG = new Logger(FromFileTestQueryHelper.class);
  private final QueryResourceTestClient queryClient;
  private final ObjectMapper jsonMapper;

  @Inject
  FromFileTestQueryHelper(ObjectMapper jsonMapper, QueryResourceTestClient queryClient)
  {
    this.jsonMapper = jsonMapper;
    this.queryClient = queryClient;
  }

  public void testQueriesFromFile(String filePath, int timesToRun) throws Exception
  {
    LOG.info("Starting query tests for [%s]", filePath);
    List<QueryWithResults> queries =
        jsonMapper.readValue(
            FromFileTestQueryHelper.class.getResourceAsStream(filePath),
            new TypeReference<List<QueryWithResults>>()
            {
            }
        );
    for (int i = 0; i < timesToRun; i++) {
      LOG.info("Starting Iteration " + i);

      boolean failed = false;
      for (QueryWithResults queryWithResult : queries) {
        LOG.info("Running Query " + queryWithResult.getQuery().getType());
        List<Map<String, Object>> result = queryClient.query(queryWithResult.getQuery());
        if (!QueryResultVerifier.compareResults(result, queryWithResult.getExpectedResults())) {
          LOG.error(
              "Failed while executing %s actualResults : %s",
              queryWithResult,
              jsonMapper.writeValueAsString(result)
          );
          failed = true;
        } else {
          LOG.info("Results Verified for Query " + queryWithResult.getQuery().getType());
        }
      }

      if (failed) {
        throw new ISE("one or more twitter  queries failed");
      }
    }
  }
}
