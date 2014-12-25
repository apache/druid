/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
