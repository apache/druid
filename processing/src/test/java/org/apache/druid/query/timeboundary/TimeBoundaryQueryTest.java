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

package org.apache.druid.query.timeboundary;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.context.QueryContextParameters;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class TimeBoundaryQueryTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  @Test
  public void testQuerySerialization() throws IOException
  {
    Query query = Druids.newTimeBoundaryQueryBuilder()
                        .dataSource("testing")
                        .build();

    String json = JSON_MAPPER.writeValueAsString(query);
    Query serdeQuery = JSON_MAPPER.readValue(json, Query.class);

    Assertions.assertEquals(query, serdeQuery);
  }

  @Test
  public void testContextSerde() throws Exception
  {
    final TimeBoundaryQuery query = Druids.newTimeBoundaryQueryBuilder()
                                          .dataSource("foo")
                                          .intervals("2013/2014")
                                          .context(
                                              QueryContext.ofMap(
                                                  QueryContextParameters.PRIORITY,
                                                  1,
                                                  QueryContextParameters.USE_CACHE,
                                                  true,
                                                  QueryContextParameters.POPULATE_CACHE,
                                                  true,
                                                  QueryContextParameters.FINALIZE,
                                                  true
                                              )
                                          ).build();

    final ObjectMapper mapper = new DefaultObjectMapper();

    final TimeBoundaryQuery serdeQuery = mapper.readValue(
        mapper.writeValueAsBytes(
            mapper.readValue(
                mapper.writeValueAsString(
                    query
                ), TimeBoundaryQuery.class
            )
        ), TimeBoundaryQuery.class
    );

    final QueryContext queryContext = query.context();
    Assertions.assertEquals(1, (int) queryContext.getInt(QueryContextParameters.PRIORITY.getName()));
    Assertions.assertEquals(true, queryContext.getBoolean(QueryContextParameters.USE_CACHE.getName()));
    Assertions.assertEquals(true, queryContext.getBoolean(QueryContextParameters.POPULATE_CACHE.getName()));
    Assertions.assertEquals(true, queryContext.getBoolean(QueryContextParameters.FINALIZE.getName()));
  }

  @Test
  public void testContextSerde2() throws Exception
  {
    final TimeBoundaryQuery query = Druids.newTimeBoundaryQueryBuilder()
                                          .dataSource("foo")
                                          .intervals("2013/2014")
                                          .context(
                                              ImmutableMap.of(
                                                  QueryContextParameters.PRIORITY.getName(),
                                                  "1",
                                                  QueryContextParameters.USE_CACHE.getName(),
                                                  "true",
                                                  QueryContextParameters.POPULATE_CACHE.getName(),
                                                  "true",
                                                  QueryContextParameters.FINALIZE.getName(),
                                                  "true"
                                              )
                                          ).build();

    final ObjectMapper mapper = new DefaultObjectMapper();

    final TimeBoundaryQuery serdeQuery = mapper.readValue(
        mapper.writeValueAsBytes(
            mapper.readValue(
                mapper.writeValueAsString(
                    query
                ), TimeBoundaryQuery.class
            )
        ), TimeBoundaryQuery.class
    );


    final QueryContext queryContext = query.context();
    Assertions.assertEquals("1", queryContext.get(QueryContextParameters.PRIORITY.getName()));
    Assertions.assertEquals("true", queryContext.get(QueryContextParameters.USE_CACHE.getName()));
    Assertions.assertEquals("true", queryContext.get(QueryContextParameters.POPULATE_CACHE.getName()));
    Assertions.assertEquals("true", queryContext.get(QueryContextParameters.FINALIZE.getName()));
  }
}
