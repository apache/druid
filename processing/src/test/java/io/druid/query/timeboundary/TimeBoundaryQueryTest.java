/*
 * Druid - a distributed column store.
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
 *
 * This file Copyright (C) 2014 N3TWORK, Inc. and contributed to the Druid project
 * under the Druid Corporate Contributor License Agreement.
 */

package io.druid.query.timeboundary;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Druids;
import io.druid.query.Query;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TimeBoundaryQueryTest
{
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Test
  public void testQuerySerialization() throws IOException
  {
    Query query = Druids.newTimeBoundaryQueryBuilder()
                        .dataSource("testing")
                        .build();

    String json = jsonMapper.writeValueAsString(query);
    Query serdeQuery = jsonMapper.readValue(json, Query.class);

    Assert.assertEquals(query, serdeQuery);
  }

  @Test
  public void testContextSerde() throws Exception
  {
    final TimeBoundaryQuery query = Druids.newTimeBoundaryQueryBuilder()
                                          .dataSource("foo")
                                          .intervals("2013/2014")
                                          .context(
                                              ImmutableMap.<String, Object>of(
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


    Assert.assertEquals(1, serdeQuery.getContextValue("priority"));
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
                                              ImmutableMap.<String, Object>of(
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


    Assert.assertEquals("1", serdeQuery.getContextValue("priority"));
    Assert.assertEquals("true", serdeQuery.getContextValue("useCache"));
    Assert.assertEquals("true", serdeQuery.getContextValue("populateCache"));
    Assert.assertEquals("true", serdeQuery.getContextValue("finalize"));
  }
}
