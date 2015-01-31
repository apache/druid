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

package io.druid.query.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequences;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Druids;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
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
import java.util.List;

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
