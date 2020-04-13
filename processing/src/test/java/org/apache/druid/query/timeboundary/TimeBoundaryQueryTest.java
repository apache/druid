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
import org.apache.druid.query.QueryContexts;
import org.junit.Assert;
import org.junit.Test;

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

    Assert.assertEquals(query, serdeQuery);
  }

  @Test
  public void testContextSerde() throws Exception
  {
    final TimeBoundaryQuery query = Druids.newTimeBoundaryQueryBuilder()
                                          .dataSource("foo")
                                          .intervals("2013/2014")
                                          .context(
                                              ImmutableMap.of(
                                                  "priority",
                                                  1,
                                                  "useCache",
                                                  true,
                                                  "populateCache",
                                                  true,
                                                  "finalize",
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


    Assert.assertEquals(new Integer(1), serdeQuery.getContextValue(QueryContexts.PRIORITY_KEY));
    Assert.assertEquals(true, serdeQuery.getContextValue("useCache"));
    Assert.assertEquals(true, serdeQuery.getContextValue("populateCache"));
    Assert.assertEquals(true, serdeQuery.getContextValue("finalize"));
  }

  @Test
  public void testContextSerde2() throws Exception
  {
    final TimeBoundaryQuery query = Druids.newTimeBoundaryQueryBuilder()
                                          .dataSource("foo")
                                          .intervals("2013/2014")
                                          .context(
                                              ImmutableMap.of(
                                                  "priority",
                                                  "1",
                                                  "useCache",
                                                  "true",
                                                  "populateCache",
                                                  "true",
                                                  "finalize",
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


    Assert.assertEquals("1", serdeQuery.getContextValue(QueryContexts.PRIORITY_KEY));
    Assert.assertEquals("true", serdeQuery.getContextValue("useCache"));
    Assert.assertEquals("true", serdeQuery.getContextValue("populateCache"));
    Assert.assertEquals("true", serdeQuery.getContextValue("finalize"));
  }
}
