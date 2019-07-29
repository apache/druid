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

package org.apache.druid.query.aggregation.cardinality.accurate;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.LongRoaringBitmapCollectorFactory;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;


@RunWith(Parameterized.class)
public class AccurateCardinalityAggGroupbyTest
{
  private final AggregationTestHelper helper;
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public AccurateCardinalityAggGroupbyTest(final GroupByQueryConfig config)
  {
    AccurateCardinalityModule acm = new AccurateCardinalityModule();
    acm.configure(null);
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        acm.getJacksonModules(),
        config,
        tempFolder
    );
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<?> constructorFeeder() throws IOException
  {
    final List<Object[]> constructors = new ArrayList<>();
    for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
      constructors.add(new Object[]{config});
    }
    return constructors;
  }

  @Test
  public void testAccurateCardinalityAggregatorFactorySerde() throws Exception
  {
    Sequence<Row> seq = helper.createIndexAndRunQueryOnSegment(
        new File(AccurateCardinalityAggGroupbyTest.class.getClassLoader()
                                                        .getResource("simple_test_data.tsv")
                                                        .getFile()),
        readFileFromClasspathAsString("simple_test_data_record_parser.json"),
        readFileFromClasspathAsString("simple_test_data_aggregators.json"),
        0,
        Granularities.NONE,
        1000,
        readFileFromClasspathAsString("simple_test_data_group_by_query.json")
    );

    List<Row> results = seq.toList();
    Assert.assertEquals(5, results.size());
    Assert.assertEquals(
        ImmutableList.of(
            new MapBasedRow(
                DateTimes.of("2014-10-19T00:00:00.000Z"),
                ImmutableMap.<String, Object>builder()
                    .put("uv", 291L)
                    .put("product", "product_1")
                    .build()
            ),
            new MapBasedRow(
                DateTimes.of("2014-10-19T00:00:00.000Z"),
                ImmutableMap.<String, Object>builder()
                    .put("uv", 297L)
                    .put("product", "product_2")
                    .build()
            ),
            new MapBasedRow(
                DateTimes.of("2014-10-19T00:00:00.000Z"),
                ImmutableMap.<String, Object>builder()
                    .put("uv", 309L)
                    .put("product", "product_3")
                    .build()
            ),
            new MapBasedRow(
                DateTimes.of("2014-10-19T00:00:00.000Z"),
                ImmutableMap.<String, Object>builder()
                    .put("uv", 300L)
                    .put("product", "product_4")
                    .build()
            ),
            new MapBasedRow(
                DateTimes.of("2014-10-19T00:00:00.000Z"),
                ImmutableMap.<String, Object>builder()
                    .put("uv", 297L)
                    .put("product", "product_5")
                    .build()
            )
        ),
        results
    );

  }

  public static String readFileFromClasspathAsString(String fileName) throws IOException
  {
    return Files.asCharSource(
        new File(AccurateCardinalityAggGroupbyTest.class.getClassLoader().getResource(fileName).getFile()),
        Charset.forName("UTF-8")
    ).read();
  }

  @Test
  public void testAccurateCardinalityAggFactorySerde() throws Exception
  {
    assertAggregatorFactorySerde(new AccurateCardinalityAggregatorFactory(
        "name",
        "fieldName",
        new LongRoaringBitmapCollectorFactory()
    ));
  }

  private void assertAggregatorFactorySerde(AggregatorFactory agg) throws Exception
  {
    Assert.assertEquals(
        agg,
        helper.getObjectMapper().readValue(
            helper.getObjectMapper().writeValueAsString(agg),
            AggregatorFactory.class
        )
    );
  }

  @Test
  public void testCacheKey()
  {
    LongRoaringBitmapCollectorFactory collectorFactory = new LongRoaringBitmapCollectorFactory();
    final AccurateCardinalityAggregatorFactory factory1 = new AccurateCardinalityAggregatorFactory(
        "name",
        "fieldName",
        collectorFactory
    );
    final AccurateCardinalityAggregatorFactory factory2 = new AccurateCardinalityAggregatorFactory(
        "name",
        "fieldName",
        collectorFactory
    );
    final AccurateCardinalityAggregatorFactory factory3 = new AccurateCardinalityAggregatorFactory(
        "name",
        "fieldName1",
        collectorFactory
    );
    Assert.assertTrue(Arrays.equals(factory1.getCacheKey(), factory2.getCacheKey()));
    Assert.assertFalse(Arrays.equals(factory1.getCacheKey(), factory3.getCacheKey()));
  }


}
