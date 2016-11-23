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
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularities;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.aggregation.AggregationTestHelper;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryRunnerTest;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 */
@RunWith(Parameterized.class)
public class QuantilesSketchAggregationGroupByQueryTest
{
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private GroupByQueryConfig config;
  private QuantilesSketchModule sm;
  private File s1;
  private File s2;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<?> constructorFeeder() throws IOException
  {
    final List<Object[]> constructors = Lists.newArrayList();
    for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
      constructors.add(new Object[]{config});
    }
    return constructors;
  }

  public QuantilesSketchAggregationGroupByQueryTest(final GroupByQueryConfig config)
  {
    this.config = config;
  }

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
        5000
    );
  }


  @Test
  public void testGpByQuery() throws Exception
  {
    AggregationTestHelper gpByQueryAggregationTestHelper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        sm.getJacksonModules(),
        config,
        tempFolder
    );

    Sequence seq = gpByQueryAggregationTestHelper.runQueryOnSegments(
        ImmutableList.of(s1, s2),
        readFileFromClasspathAsString("quantiles/group_by_query.json")
    );

    List<Row> results = Sequences.toList(seq, Lists.<Row>newArrayList());
    Assert.assertEquals(1, results.size());

    MapBasedRow resultRow = (MapBasedRow) Iterables.getOnlyElement(results);
    Assert.assertEquals(DateTime.parse("2014-10-19T00:00:00.000Z"), resultRow.getTimestamp());

    Map<String, Object> resultEvent = resultRow.getEvent();
    Assert.assertEquals(6720L, resultEvent.get("valueSketch"));
    Assert.assertEquals(0L, resultEvent.get("non_existing_col_validation"));
    Assert.assertEquals(100.0d, ((Double)resultEvent.get("valueMin")).doubleValue(), 1.0001);
    Assert.assertEquals(3459.0d, ((Double)resultEvent.get("valueMax")).doubleValue(), 1.0001);
    Assert.assertEquals(941.0d, ((Double)resultEvent.get("valueQuantile")).doubleValue(), 1.0001);
    Assert.assertArrayEquals(
        new double[]{100.0, 941.0, 1781.0, 2620.0, 3459.0},
        (double[]) resultEvent.get("valueQuantiles"),
        1.0001
    );
    Assert.assertArrayEquals(
        new double[]{1680.0, 3360.0, 1680.0},
        (double[]) resultEvent.get("valueCustomSplitsHistogram"),
        1.0001
    );
    Assert.assertArrayEquals(
        new double[]{3360.0, 3360.0},
        (double[]) resultEvent.get("valueEqualSplitsHistogram"),
        1.0001
    );
  }

  private final static String readFileFromClasspathAsString(String fileName) throws IOException
  {
    return Files.asCharSource(
        new File(QuantilesSketchAggregationGroupByQueryTest.class.getClassLoader().getResource(fileName).getFile()),
        Charset.forName("UTF-8")
    ).read();
  }
}
