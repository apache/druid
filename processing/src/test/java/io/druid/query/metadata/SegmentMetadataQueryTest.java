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

package io.druid.query.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Query;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

public class SegmentMetadataQueryTest
{
  private ObjectMapper mapper = new DefaultObjectMapper();

  @Test
  public void testSerde() throws Exception
  {
    String queryStr = "{\n"
                      + "  \"queryType\":\"segmentMetadata\",\n"
                      + "  \"dataSource\":\"test_ds\",\n"
                      + "  \"intervals\":[\"2013-12-04T00:00:00.000Z/2013-12-05T00:00:00.000Z\"]\n"
                      + "}";
    Query query = mapper.readValue(queryStr, Query.class);
    Assert.assertTrue(query instanceof SegmentMetadataQuery);
    Assert.assertEquals("test_ds", query.getDataSource().getName());
    Assert.assertEquals(new Interval("2013-12-04T00:00:00.000Z/2013-12-05T00:00:00.000Z"), query.getIntervals().get(0));

    // test serialize and deserialize
    Assert.assertEquals(query, mapper.readValue(mapper.writeValueAsString(query), Query.class));

  }
}
