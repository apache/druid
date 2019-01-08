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

import com.google.common.collect.Lists;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.guice.BloomFilterExtensionModule;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.filter.BloomKFilter;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class BloomFilterGroupByQueryTest
{
  private AggregationTestHelper helper;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public BloomFilterGroupByQueryTest(final GroupByQueryConfig config)
  {
    BloomFilterExtensionModule module = new BloomFilterExtensionModule();
    module.configure(null);
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        Lists.newArrayList(module.getJacksonModules()),
        config,
        tempFolder
    );
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
  public void testIngestWithNullsIgnoredAndQuery() throws Exception
  {
    MapBasedRow row = ingestAndQuery(true);
    Object o = row.getRaw("blooming_quality");
    Assert.assertTrue(((BloomKFilter) row.getRaw("blooming_quality")).testString("mezzanine"));
    Assert.assertTrue(((BloomKFilter) row.getRaw("blooming_quality")).testString("premium"));
    Assert.assertFalse(((BloomKFilter) row.getRaw("blooming_quality")).testString("entertainment"));

  }

  @Test
  public void testIngestWithNullsToZeroAndQuery() throws Exception
  {
    // Nulls are ignored and not replaced with default for SQL compatible null handling.
    // This is already tested in testIngestWithNullsIgnoredAndQuery()
    if (NullHandling.replaceWithDefault()) {
      MapBasedRow row = ingestAndQuery(false);
      Assert.assertTrue(((BloomKFilter) row.getRaw("blooming_quality")).testString("mezzanine"));
      Assert.assertTrue(((BloomKFilter) row.getRaw("blooming_quality")).testString("premium"));
      Assert.assertFalse(((BloomKFilter) row.getRaw("blooming_quality")).testString("entertainment"));
    }
  }

  private MapBasedRow ingestAndQuery(boolean ignoreNulls) throws Exception
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

    Sequence seq = helper.createIndexAndRunQueryOnSegment(
        this.getClass().getClassLoader().getResourceAsStream("sample.data.tsv"),
        parseSpec,
        metricSpec,
        0,
        Granularities.NONE,
        50000,
        query
    );

    return (MapBasedRow) seq.toList().get(0);
  }
}
