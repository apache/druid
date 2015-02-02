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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequences;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Druids;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.metadata.metadata.ColumnAnalysis;
import io.druid.query.metadata.metadata.ListColumnIncluderator;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.TestIndex;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;

public class SegmentMetadataQueryTest
{
  @SuppressWarnings("unchecked")
  private final QueryRunner runner = makeQueryRunner(
      new SegmentMetadataQueryRunnerFactory(
          new SegmentMetadataQueryQueryToolChest(),
          QueryRunnerTestHelper.NOOP_QUERYWATCHER)
  );
  private ObjectMapper mapper = new DefaultObjectMapper();

  @SuppressWarnings("unchecked")
  public static QueryRunner makeQueryRunner(
      QueryRunnerFactory factory
  )
  {
    return QueryRunnerTestHelper.makeQueryRunner(
        factory,
        new QueryableIndexSegment(QueryRunnerTestHelper.segmentId, TestIndex.getMMappedTestIndex())
    );
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSegmentMetadataQuery()
  {
    SegmentMetadataQuery query = Druids.newSegmentMetadataQueryBuilder()
                                       .dataSource("testing")
                                       .intervals("2013/2014")
                                       .toInclude(new ListColumnIncluderator(Arrays.asList("placement")))
                                       .merge(true)
                                       .build();
    HashMap<String,Object> context = new HashMap<String, Object>();
    Iterable<SegmentAnalysis> results = Sequences.toList(
        runner.run(query, context),
        Lists.<SegmentAnalysis>newArrayList()
    );
    SegmentAnalysis val = results.iterator().next();
    Assert.assertEquals("testSegment", val.getId());
    Assert.assertEquals(69843, val.getSize());
    Assert.assertEquals(
        Arrays.asList(new Interval("2011-01-12T00:00:00.000Z/2011-04-15T00:00:00.001Z")),
        val.getIntervals()
    );
    Assert.assertEquals(1, val.getColumns().size());
    final ColumnAnalysis columnAnalysis = val.getColumns().get("placement");
    Assert.assertEquals("STRING", columnAnalysis.getType());
    Assert.assertEquals(10881, columnAnalysis.getSize());
    Assert.assertEquals(new Integer(1), columnAnalysis.getCardinality());
    Assert.assertNull(columnAnalysis.getErrorMessage());

  }

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
    Assert.assertEquals("test_ds", Iterables.getOnlyElement(query.getDataSource().getNames()));
    Assert.assertEquals(new Interval("2013-12-04T00:00:00.000Z/2013-12-05T00:00:00.000Z"), query.getIntervals().get(0));

    // test serialize and deserialize
    Assert.assertEquals(query, mapper.readValue(mapper.writeValueAsString(query), Query.class));

  }
}
