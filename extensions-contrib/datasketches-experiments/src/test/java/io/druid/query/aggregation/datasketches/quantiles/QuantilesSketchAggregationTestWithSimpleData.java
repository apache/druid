/*
* Licensed to Metamarkets Group Inc. (Metamarkets) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. Metamarkets licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package io.druid.query.aggregation.datasketches.quantiles;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import io.druid.granularity.QueryGranularities;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregationTestHelper;
import io.druid.query.select.SelectResultValue;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.query.topn.DimensionAndMetricValueExtractor;
import io.druid.query.topn.TopNResultValue;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

/**
 */
public class QuantilesSketchAggregationTestWithSimpleData
{
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private QuantilesSketchModule sm;
  private File s1;
  private File s2;

  @Before
  public void setup() throws Exception
  {
    sm = new QuantilesSketchModule();
    sm.configure(null);
    AggregationTestHelper aggTestHelper = AggregationTestHelper.createSelectQueryAggregationTestHelper(
        sm.getJacksonModules(),
        tempFolder
    );

    s1 = tempFolder.newFolder();
    aggTestHelper.createIndex(
        new File(this.getClass().getClassLoader().getResource("quantiles/quantiles_test_data.tsv").getFile()),
        readFileFromClasspathAsString("quantiles/quantiles_test_data_record_parser.json"),
        readFileFromClasspathAsString("quantiles/quantiles_test_data_aggregators.json"),
        s1,
        0,
        QueryGranularities.NONE,
        5
    );

    s2 = tempFolder.newFolder();
    aggTestHelper.createIndex(
        new File(this.getClass().getClassLoader().getResource("quantiles/quantiles_test_data.tsv").getFile()),
        readFileFromClasspathAsString("quantiles/quantiles_test_data_record_parser.json"),
        readFileFromClasspathAsString("quantiles/quantiles_test_data_aggregators.json"),
        s2,
        0,
        QueryGranularities.NONE,
        5
    );
  }

  @Test
  public void testSelectQuery() throws Exception
  {
    AggregationTestHelper selectQueryAggregationTestHelper = AggregationTestHelper.createSelectQueryAggregationTestHelper(
        sm.getJacksonModules(),
        tempFolder
    );

    Sequence seq = selectQueryAggregationTestHelper.runQueryOnSegments(
        ImmutableList.of(s1, s2),
        readFileFromClasspathAsString("quantiles/select_query.json")
    );

    Result<SelectResultValue> result = (Result<SelectResultValue>) Iterables.getOnlyElement(Sequences.toList(seq, Lists.newArrayList()));
    Assert.assertEquals(new DateTime("2014-10-20T00:00:00.000Z"), result.getTimestamp());
    Assert.assertEquals(100, result.getValue().getEvents().size());
    Assert.assertEquals(
        "AgMICAAEAAABAAAAAAAAAAAAAAAAAFlAAAAAAAAAWUAAAAAAAABZQA==",
        result.getValue().getEvents().get(0).getEvent().get("value")
    );
  }

  @Test
  public void testTopNQuery() throws Exception
  {
    AggregationTestHelper topNQueryAggregationTestHelper = AggregationTestHelper.createTopNQueryAggregationTestHelper(
        sm.getJacksonModules(),
        tempFolder
    );

    Sequence seq = topNQueryAggregationTestHelper.runQueryOnSegments(
        ImmutableList.of(s1, s2),
        readFileFromClasspathAsString("quantiles/topN_query.json")
    );

    Result<TopNResultValue> result = (Result<TopNResultValue>) Iterables.getOnlyElement(
        Sequences.toList(seq, Lists.newArrayList())
    );

    Assert.assertEquals(new DateTime("2014-10-20T00:00:00.000Z"), result.getTimestamp());

    DimensionAndMetricValueExtractor value = Iterables.getOnlyElement(result.getValue().getValue());

    Assert.assertEquals(6720L, value.getLongMetric("valueSketch").longValue());
    Assert.assertEquals(0L, value.getLongMetric("non_existing_col_validation").longValue());
    Assert.assertEquals(100.0d, (value.getDoubleMetric("valueMin")).doubleValue(), 1.0001);
    Assert.assertEquals(3459.0d, (value.getDoubleMetric("valueMax")).doubleValue(), 1.0001);
    Assert.assertEquals(941.0d, (value.getDoubleMetric("valueQuantile")).doubleValue(), 1.0001);
    Assert.assertArrayEquals(
        new double[]{100.0, 941.0, 1781.0, 2620.0, 3459.0},
        (double[]) value.getDimensionValue("valueQuantiles"),
        1.0001
    );
    Assert.assertArrayEquals(
        new double[]{1680.0, 3360.0, 1680.0},
        (double[]) value.getDimensionValue("valueCustomSplitsHistogram"),
        1.0001
    );
    Assert.assertArrayEquals(
        new double[]{3360.0, 3360.0},
        (double[]) value.getDimensionValue("valueEqualSplitsHistogram"),
        1.0001
    );
  }

  @Test
  public void testTimeseriesQuery() throws Exception
  {
    AggregationTestHelper timeseriesQueryAggregationTestHelper = AggregationTestHelper.createTimeseriesQueryAggregationTestHelper(
        sm.getJacksonModules(),
        tempFolder
    );

    Sequence seq = timeseriesQueryAggregationTestHelper.runQueryOnSegments(
        ImmutableList.of(s1, s2),
        readFileFromClasspathAsString("quantiles/timeseries_query.json")
    );

    Result<TimeseriesResultValue> resultRow = (Result<TimeseriesResultValue>) Iterables.getOnlyElement(
        Sequences.toList(seq, Lists.newArrayList())
    );

    Assert.assertEquals(DateTime.parse("2014-10-20T00:00:00.000Z"), resultRow.getTimestamp());

    TimeseriesResultValue value = resultRow.getValue();
    Assert.assertEquals(6720L, value.getLongMetric("valueSketch").longValue());
    Assert.assertEquals(0L, value.getLongMetric("non_existing_col_validation").longValue());
    Assert.assertEquals(100.0d, value.getDoubleMetric("valueMin").doubleValue(), 1.0001);
    Assert.assertEquals(3459.0d, value.getDoubleMetric("valueMax").doubleValue(), 1.0001);
    Assert.assertEquals(941.0d, value.getDoubleMetric("valueQuantile").doubleValue(), 1.0001);
    Assert.assertArrayEquals(
        new double[]{100.0, 941.0, 1781.0, 2620.0, 3459.0},
        (double[]) value.getMetric("valueQuantiles"),
        1.0001
    );
    Assert.assertArrayEquals(
        new double[]{1680.0, 3360.0, 1680.0},
        (double[]) value.getMetric("valueCustomSplitsHistogram"),
        1.0001
    );
    Assert.assertArrayEquals(
        new double[]{3360.0, 3360.0},
        (double[]) value.getMetric("valueEqualSplitsHistogram"),
        1.0001
    );
  }

  public final static String readFileFromClasspathAsString(String fileName) throws IOException
  {
    return Files.asCharSource(
        new File(QuantilesSketchAggregationTestWithSimpleData.class.getClassLoader().getResource(fileName).getFile()),
        Charset.forName("UTF-8")
    ).read();
  }
}
