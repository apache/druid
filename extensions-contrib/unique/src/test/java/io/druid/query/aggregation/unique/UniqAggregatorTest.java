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

package io.druid.query.aggregation.unique;

import io.druid.data.input.Row;
import io.druid.initialization.DruidModule;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.guava.Accumulators;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.aggregation.AggregationTestHelper;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryRunnerTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class UniqAggregatorTest
{
  private final AggregationTestHelper helper;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public UniqAggregatorTest(GroupByQueryConfig config)
  {
    DruidModule module = new DruidUniqueModule();
    module.configure(null);
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        module.getJacksonModules(), config, tempFolder);
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


  @Test
  public void buildBitmapAtIngestionTime() throws Exception
  {
    Sequence<Row> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("unique.tsv").getFile()),
        String.join(
            "\n",
            "{",
            "  \"type\": \"string\",",
            "  \"parseSpec\": {",
            "    \"format\": \"tsv\",",
            "    \"timestampSpec\": {\"column\": \"timestamp\", \"format\": \"yyyyMMdd\"},",
            "    \"dimensionsSpec\": {",
            "      \"dimensions\": [\"dim\",\"id\"],",
            "      \"dimensionExclusions\": [],",
            "      \"spatialDimensions\": []",
            "    },",
            "    \"columns\": [\"timestamp\", \"dim\", \"id\"]",
            "  }",
            "}"
        ),
        String.join(
            "\n",
            "[",
            "  {\"type\": \"unique\", \"name\": \"unique\", \"fieldName\": \"id\"}",
            "]"
        ),
        0, // minTimestamp
        Granularities.NONE,
        100, // maxRowCount
        String.join(
            "\n",
            "{",
            "  \"queryType\": \"groupBy\",",
            "  \"dataSource\": \"test_datasource\",",
            "  \"granularity\": \"ALL\",",
            "  \"dimensions\": [],",
            "  \"aggregations\": [",
            "    {\"type\": \"unique\", \"name\": \"unique\", \"fieldName\": \"unique\"}",
            "  ],",
            "  \"intervals\": [\"2017-01-01T00:00:00.000Z/2017-01-31T00:00:00.000Z\"]",
            "}"
        )
    );
    List<Row> results = seq.accumulate(new ArrayList<>(), Accumulators.list());
    Assert.assertEquals(1, results.size());
    Row row = results.get(0);
    Assert.assertEquals(200, row.getMetric("unique"));
  }
}
