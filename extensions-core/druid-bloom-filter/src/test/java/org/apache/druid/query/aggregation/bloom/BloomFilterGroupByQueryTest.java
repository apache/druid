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

package org.apache.druid.query.aggregation.bloom;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Key;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.guice.BloomFilterExtensionModule;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.filter.BloomKFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.segment.TestHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class BloomFilterGroupByQueryTest
{
  private static final BloomFilterExtensionModule MODULE = new BloomFilterExtensionModule();

  static {
    // throwaway, just using to properly initialize jackson modules
    Guice.createInjector(
        binder -> binder.bind(Key.get(ObjectMapper.class, Json.class)).toInstance(TestHelper.makeJsonMapper()),
        MODULE
    );
  }

  private AggregationTestHelper helper;
  private boolean isV2;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public BloomFilterGroupByQueryTest(final GroupByQueryConfig config)
  {
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        Lists.newArrayList(MODULE.getJacksonModules()),
        config,
        tempFolder
    );
    isV2 = config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V2);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
      constructors.add(new Object[]{config});
    }
    return constructors;
  }

  @After
  public void teardown() throws IOException
  {
    helper.close();
  }

  @Test
  public void testQuery() throws Exception
  {
    String query = "{"
                   + "\"queryType\": \"groupBy\","
                   + "\"dataSource\": \"test_datasource\","
                   + "\"granularity\": \"ALL\","
                   + "\"dimensions\": [],"
                   + "\"filter\":{ \"type\":\"selector\", \"dimension\":\"market\", \"value\":\"upfront\"},"
                   + "\"aggregations\": ["
                   + "  { \"type\": \"bloom\", \"name\": \"blooming_quality\", \"field\": \"quality\" }"
                   + "],"
                   + "\"intervals\": [ \"1970/2050\" ]"
                   + "}";

    MapBasedRow row = ingestAndQuery(query);


    BloomKFilter filter = BloomKFilter.deserialize((ByteBuffer) row.getRaw("blooming_quality"));
    Assert.assertTrue(filter.testString("mezzanine"));
    Assert.assertTrue(filter.testString("premium"));
    Assert.assertFalse(filter.testString("entertainment"));
  }

  @Test
  public void testNestedQuery() throws Exception
  {
    if (!isV2) {
      return;
    }

    String query = "{"
                   + "\"queryType\": \"groupBy\","
                   + "\"dataSource\": {"
                   + "\"type\": \"query\","
                   + "\"query\": {"
                   + "\"queryType\":\"groupBy\","
                   + "\"dataSource\": \"test_datasource\","
                   + "\"intervals\": [ \"1970/2050\" ],"
                   + "\"granularity\":\"ALL\","
                   + "\"dimensions\":[],"
                   + "\"aggregations\": [{ \"type\":\"longSum\", \"name\":\"innerSum\", \"fieldName\":\"count\"}]"
                   + "}"
                   + "},"
                   + "\"granularity\": \"ALL\","
                   + "\"dimensions\": [],"
                   + "\"aggregations\": ["
                   + "  { \"type\": \"bloom\", \"name\": \"bloom\", \"field\": \"innerSum\" }"
                   + "],"
                   + "\"intervals\": [ \"1970/2050\" ]"
                   + "}";

    MapBasedRow row = ingestAndQuery(query);


    BloomKFilter filter = BloomKFilter.deserialize((ByteBuffer) row.getRaw("bloom"));
    Assert.assertTrue(filter.testLong(13L));
    Assert.assertFalse(filter.testLong(5L));
  }


  @Test
  public void testNestedQueryComplex() throws Exception
  {
    if (!isV2) {
      return;
    }

    String query = "{"
                   + "\"queryType\": \"groupBy\","
                   + "\"dataSource\": {"
                   + "\"type\": \"query\","
                   + "\"query\": {"
                   + "\"queryType\":\"groupBy\","
                   + "\"dataSource\": \"test_datasource\","
                   + "\"intervals\": [ \"1970/2050\" ],"
                   + "\"granularity\":\"ALL\","
                   + "\"dimensions\":[],"
                   + "\"filter\":{ \"type\":\"selector\", \"dimension\":\"market\", \"value\":\"upfront\"},"
                   + "\"aggregations\": [{ \"type\":\"bloom\", \"name\":\"innerBloom\", \"field\":\"quality\"}]"
                   + "}"
                   + "},"
                   + "\"granularity\": \"ALL\","
                   + "\"dimensions\": [],"
                   + "\"aggregations\": ["
                   + "  { \"type\": \"bloom\", \"name\": \"innerBloom\", \"field\": \"innerBloom\" }"
                   + "],"
                   + "\"intervals\": [ \"1970/2050\" ]"
                   + "}";

    MapBasedRow row = ingestAndQuery(query);


    BloomKFilter filter = BloomKFilter.deserialize((ByteBuffer) row.getRaw("innerBloom"));
    Assert.assertTrue(filter.testString("mezzanine"));
    Assert.assertTrue(filter.testString("premium"));
    Assert.assertFalse(filter.testString("entertainment"));
  }

  @Test
  public void testQueryFakeDimension() throws Exception
  {
    String query = "{"
                   + "\"queryType\": \"groupBy\","
                   + "\"dataSource\": \"test_datasource\","
                   + "\"granularity\": \"ALL\","
                   + "\"dimensions\": [],"
                   + "\"filter\":{ \"type\":\"selector\", \"dimension\":\"market\", \"value\":\"upfront\"},"
                   + "\"aggregations\": ["
                   + "  { \"type\": \"bloom\", \"name\": \"blooming_quality\", \"field\": \"nope\" }"
                   + "],"
                   + "\"intervals\": [ \"1970/2050\" ]"
                   + "}";

    MapBasedRow row = ingestAndQuery(query);

    // a nil column results in a totally empty bloom filter
    BloomKFilter filter = new BloomKFilter(1500);

    Object val = row.getRaw("blooming_quality");

    String serialized = BloomFilterAggregatorTest.filterToString(BloomKFilter.deserialize((ByteBuffer) val));
    String empty = BloomFilterAggregatorTest.filterToString(filter);

    Assert.assertEquals(empty, serialized);
  }

  private MapBasedRow ingestAndQuery(String query) throws Exception
  {
    String metricSpec = "[{ \"type\": \"count\", \"name\": \"count\"}]";

    String parseSpec = "{"
                       + "\"type\" : \"string\","
                       + "\"parseSpec\" : {"
                       + "    \"format\" : \"tsv\","
                       + "    \"timestampSpec\" : {"
                       + "        \"column\" : \"timestamp\","
                       + "        \"format\" : \"auto\""
                       + "},"
                       + "    \"dimensionsSpec\" : {"
                       + "        \"dimensions\": [],"
                       + "        \"dimensionExclusions\" : [],"
                       + "        \"spatialDimensions\" : []"
                       + "    },"
                       + "    \"columns\": [\"timestamp\", \"market\", \"quality\", \"placement\", \"placementish\", \"index\"]"
                       + "  }"
                       + "}";

    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        this.getClass().getClassLoader().getResourceAsStream("sample.data.tsv"),
        parseSpec,
        metricSpec,
        0,
        Granularities.NONE,
        50000,
        query
    );

    return seq.toList().get(0).toMapBasedRow((GroupByQuery) helper.readQuery(query));
  }
}
