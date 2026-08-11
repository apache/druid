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
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.impl.DelimitedInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.guice.BloomFilterExtensionModule;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.BloomKFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class BloomFilterGroupByQueryTest extends InitializedNullHandlingTest
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

  @TempDir
  private File tempFolder;

  public void initBloomFilterGroupByQueryTest(final GroupByQueryConfig config)
  {
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelperWithTempDir(
        Lists.newArrayList(MODULE.getJacksonModules()),
        config,
        tempFolder
    );
  }

  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
      constructors.add(new Object[]{config});
    }
    return constructors;
  }

  @AfterEach
  public void teardown() throws IOException
  {
    if (helper != null) {
      helper.close();
    }
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testQuery(final GroupByQueryConfig config) throws Exception
  {
    initBloomFilterGroupByQueryTest(config);
    GroupByQuery query = GroupByQuery.builder()
                                     .setDataSource("test_datasource")
                                     .setGranularity(Granularities.ALL)
                                     .setInterval("1970/2050")
                                     .setDimFilter(new SelectorDimFilter("market", "upfront", null))
                                     .setAggregatorSpecs(
                                         new BloomFilterAggregatorFactory("blooming_quality", new DefaultDimensionSpec("quality", "quality"), null)
                                     )
                                     .build();

    MapBasedRow row = ingestAndQuery(query);

    BloomKFilter filter = BloomKFilter.deserialize((ByteBuffer) row.getRaw("blooming_quality"));
    Assertions.assertTrue(filter.testString("mezzanine"));
    Assertions.assertTrue(filter.testString("premium"));
    Assertions.assertFalse(filter.testString("entertainment"));
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testNestedQuery(final GroupByQueryConfig config) throws Exception
  {
    initBloomFilterGroupByQueryTest(config);
    GroupByQuery innerQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval("1970/2050")
                                          .setAggregatorSpecs(new LongSumAggregatorFactory("innerSum", "count"))
                                          .build();

    GroupByQuery query = GroupByQuery.builder()
                                     .setDataSource(innerQuery)
                                     .setGranularity(Granularities.ALL)
                                     .setInterval("1970/2050")
                                     .setAggregatorSpecs(
                                         new BloomFilterAggregatorFactory("bloom", new DefaultDimensionSpec("innerSum", "innerSum"), null)
                                     )
                                     .build();

    MapBasedRow row = ingestAndQuery(query);

    BloomKFilter filter = BloomKFilter.deserialize((ByteBuffer) row.getRaw("bloom"));
    Assertions.assertTrue(filter.testLong(13L));
    Assertions.assertFalse(filter.testLong(5L));
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testNestedQueryComplex(final GroupByQueryConfig config) throws Exception
  {
    initBloomFilterGroupByQueryTest(config);
    GroupByQuery innerQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval("1970/2050")
                                          .setDimFilter(new SelectorDimFilter("market", "upfront", null))
                                          .setAggregatorSpecs(
                                              new BloomFilterAggregatorFactory("innerBloom", new DefaultDimensionSpec("quality", "quality"), null)
                                          )
                                          .build();

    GroupByQuery query = GroupByQuery.builder()
                                     .setDataSource(innerQuery)
                                     .setGranularity(Granularities.ALL)
                                     .setInterval("1970/2050")
                                     .setAggregatorSpecs(
                                         new BloomFilterAggregatorFactory("innerBloom", new DefaultDimensionSpec("innerBloom", "innerBloom"), null)
                                     )
                                     .build();

    MapBasedRow row = ingestAndQuery(query);

    BloomKFilter filter = BloomKFilter.deserialize((ByteBuffer) row.getRaw("innerBloom"));
    Assertions.assertTrue(filter.testString("mezzanine"));
    Assertions.assertTrue(filter.testString("premium"));
    Assertions.assertFalse(filter.testString("entertainment"));
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testQueryFakeDimension(final GroupByQueryConfig config) throws Exception
  {
    initBloomFilterGroupByQueryTest(config);
    GroupByQuery query = GroupByQuery.builder()
                                     .setDataSource("test_datasource")
                                     .setGranularity(Granularities.ALL)
                                     .setInterval("1970/2050")
                                     .setDimFilter(new SelectorDimFilter("market", "upfront", null))
                                     .setAggregatorSpecs(
                                         new BloomFilterAggregatorFactory("blooming_quality", new DefaultDimensionSpec("nope", "nope"), null)
                                     )
                                     .build();

    MapBasedRow row = ingestAndQuery(query);

    // a nil column results in a totally empty bloom filter
    BloomKFilter filter = new BloomKFilter(1500);

    Object val = row.getRaw("blooming_quality");

    String serialized = BloomFilterAggregatorTest.filterToString(BloomKFilter.deserialize((ByteBuffer) val));
    String empty = BloomFilterAggregatorTest.filterToString(filter);

    Assertions.assertEquals(empty, serialized);
  }

  private MapBasedRow ingestAndQuery(GroupByQuery query) throws Exception
  {
    List<AggregatorFactory> metricSpec = List.of(new CountAggregatorFactory("count"));

    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        this.getClass().getClassLoader().getResourceAsStream("sample.data.tsv"),
        new InputRowSchema(
            TimestampSpec.DEFAULT,
            DimensionsSpec.EMPTY,
            ColumnsFilter.all()
        ),
        DelimitedInputFormat.forColumns(
            List.of("timestamp", "market", "quality", "placement", "placementish", "index")
        ),
        metricSpec,
        0,
        Granularities.NONE,
        50000,
        query
    );

    List<ResultRow> results = seq.toList();
    return results.get(0).toMapBasedRow(query);
  }
}
