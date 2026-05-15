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

package org.apache.druid.query.aggregation.histogram;

import com.google.common.collect.Lists;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.impl.DelimitedInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 */
@RunWith(Parameterized.class)
public class FixedBucketsHistogramAggregationTest extends InitializedNullHandlingTest
{
  private final AggregationTestHelper helper;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public FixedBucketsHistogramAggregationTest(final GroupByQueryConfig config)
  {
    ApproximateHistogramDruidModule.registerSerde();
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        Lists.newArrayList(new ApproximateHistogramDruidModule().getJacksonModules()),
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
    MapBasedRow row = ingestAndQuery(this.getClass().getClassLoader().getResourceAsStream("sample.data.tsv"));
    FixedBucketsHistogram histogram = (FixedBucketsHistogram) row.getRaw("index_fbh");
    Assert.assertEquals(5, histogram.getCount());
    Assert.assertEquals(92.782760, row.getMetric("index_min").floatValue(), 0.0001);
    Assert.assertEquals(135.109191, row.getMetric("index_max").floatValue(), 0.0001);
    Assert.assertEquals(135.9499969482422, row.getMetric("index_quantile").floatValue(), 0.0001);
  }

  /**
   * When {@link org.apache.druid.segment.RowCombiningTimeAndDimsIterator#moveToNext} is merging indexes,
   * if {@link org.apache.druid.segment.MergingRowIterator#hasTimeAndDimsChangedSinceMark} is false, then
   * {@link org.apache.druid.query.aggregation.AggregateCombiner#reset} gets called. This is the only path
   * that calls this method.
   */
  @Test
  public void testAggregateCombinerReset() throws Exception
  {
    String inputRows = "2011-04-15T00:00:00.000Z\tspot\thealth\tpreferred\ta\u0001preferred\t10\n"
                       + "2011-04-15T00:00:00.000Z\tspot\thealth\tpreferred\ta\u0001preferred\t20\n"
                       + "2011-04-15T00:00:00.000Z\tspot\thealth\tpreferred\ta\u0001preferred\t30\n"
                       + "2011-04-15T00:00:00.000Z\tspot\thealth\tpreferred\ta\u0001preferred\t40\n"
                       + "2011-04-15T00:00:00.000Z\tspot\thealth\tpreferred\ta\u0001preferred\t50\n"
                       + "2011-04-15T00:00:00.000Z\tspot\thealth\tpreferred\ta\u0001preferred\t10\n"
                       + "2011-04-15T00:00:00.000Z\tspot\thealth\tpreferred\ta\u0001preferred\t20\n"
                       + "2011-04-15T00:00:00.000Z\tspot\thealth\tpreferred\ta\u0001preferred\t30\n"
                       + "2011-04-15T00:00:00.000Z\tspot\thealth\tpreferred\ta\u0001preferred\t40\n"
                       + "2011-04-15T00:00:00.000Z\tspot\thealth\tpreferred\ta\u0001preferred\t50\n";
    MapBasedRow row = ingestAndQuery(new ByteArrayInputStream(inputRows.getBytes(StandardCharsets.UTF_8)));
    FixedBucketsHistogram histogram = (FixedBucketsHistogram) row.getRaw("index_fbh");
    Assert.assertEquals(10, histogram.getCount());
    Assert.assertEquals(10, row.getMetric("index_min").floatValue(), 0.0001);
    Assert.assertEquals(50, row.getMetric("index_max").floatValue(), 0.0001);
    // Current interpolation logic doesn't consider min/max: it assumes the values seen were evenly-distributed between 50 and 51.
    Assert.assertEquals(50.95, row.getMetric("index_quantile").floatValue(), 0.0001);
  }

  private MapBasedRow ingestAndQuery(InputStream inputDataStream) throws Exception
  {
    List<AggregatorFactory> metricSpec = List.of(
        new FixedBucketsHistogramAggregatorFactory(
            "index_fbh", "index", 200, 0, 200,
            FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, null
        )
    );

    GroupByQuery query = GroupByQuery.builder()
                                     .setDataSource("test_datasource")
                                     .setGranularity(Granularities.ALL)
                                     .setInterval("1970/2050")
                                     .setAggregatorSpecs(
                                         new FixedBucketsHistogramAggregatorFactory(
                                             "index_fbh", "index_fbh", 200, 0, 200,
                                             FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, true
                                         )
                                     )
                                     .setPostAggregatorSpecs(
                                         new MinPostAggregator("index_min", "index_fbh"),
                                         new MaxPostAggregator("index_max", "index_fbh"),
                                         new QuantilePostAggregator("index_quantile", "index_fbh", 0.99f)
                                     )
                                     .build();

    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        inputDataStream,
        new InputRowSchema(
            new TimestampSpec("timestamp", "auto", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(List.of())),
            ColumnsFilter.all()
        ),
        DelimitedInputFormat.forColumns(
            List.of("timestamp", "market", "quality", "placement", "placementish", "index")
        ),
        metricSpec,
        0,
        Granularities.NONE,
        5, // ensure we get more than one index, to test merging
        query
    );

    return seq.toList().get(0).toMapBasedRow(query);
  }
}
