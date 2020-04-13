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

package org.apache.druid.query.aggregation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipFile;

@RunWith(Parameterized.class)
public class TimestampGroupByAggregationTest
{
  private AggregationTestHelper helper;

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private ColumnSelectorFactory selectorFactory;
  private TestObjectColumnSelector selector;

  private Timestamp[] values = new Timestamp[10];

  @Parameterized.Parameters(name = "{index}: Test for {0}, config = {1}")
  public static Iterable<Object[]> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();

    final List<List<Object>> partialConstructors = ImmutableList.of(
        ImmutableList.of("timeMin", "tmin", "time_min", DateTimes.of("2011-01-12T01:00:00.000Z")),
        ImmutableList.of("timeMax", "tmax", "time_max", DateTimes.of("2011-01-31T01:00:00.000Z"))
    );

    for (final List<Object> partialConstructor : partialConstructors) {
      for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
        final List<Object> constructor = Lists.newArrayList(partialConstructor);
        constructor.add(config);
        constructors.add(constructor.toArray());
      }
    }

    return constructors;
  }

  private final String aggType;
  private final String aggField;
  private final String groupByField;
  private final DateTime expected;
  private final GroupByQueryConfig config;

  public TimestampGroupByAggregationTest(
      String aggType,
      String aggField,
      String groupByField,
      DateTime expected,
      GroupByQueryConfig config
  )
  {
    this.aggType = aggType;
    this.aggField = aggField;
    this.groupByField = groupByField;
    this.expected = expected;
    this.config = config;
  }

  @Before
  public void setup()
  {
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        new TimestampMinMaxModule().getJacksonModules(),
        config,
        temporaryFolder
    );

    selector = new TestObjectColumnSelector<>(values);
    selectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(selectorFactory.makeColumnValueSelector("test")).andReturn(selector);
    EasyMock.replay(selectorFactory);
  }

  @After
  public void teardown() throws IOException
  {
    helper.close();
  }

  @Test
  public void testSimpleDataIngestionAndGroupByTest() throws Exception
  {
    String recordParser = "{\n" +
        "  \"type\": \"string\",\n" +
        "  \"parseSpec\": {\n" +
        "    \"format\": \"tsv\",\n" +
        "    \"timestampSpec\": {\n" +
        "      \"column\": \"timestamp\",\n" +
        "      \"format\": \"auto\"\n" +
        "    },\n" +
        "    \"dimensionsSpec\": {\n" +
        "      \"dimensions\": [\n" +
        "        \"product\"\n" +
        "      ],\n" +
        "      \"dimensionExclusions\": [],\n" +
        "      \"spatialDimensions\": []\n" +
        "    },\n" +
        "    \"columns\": [\n" +
        "      \"timestamp\",\n" +
        "      \"cat\",\n" +
        "      \"product\",\n" +
        "      \"prefer\",\n" +
        "      \"prefer2\",\n" +
        "      \"pty_country\"\n" +
        "    ]\n" +
        "  }\n" +
        "}";
    String aggregator = "[\n" +
        "  {\n" +
        "    \"type\": \"" + aggType + "\",\n" +
        "    \"name\": \"" + aggField + "\",\n" +
        "    \"fieldName\": \"timestamp\"\n" +
        "  }\n" +
        "]";
    String groupBy = "{\n" +
        "  \"queryType\": \"groupBy\",\n" +
        "  \"dataSource\": \"test_datasource\",\n" +
        "  \"granularity\": \"MONTH\",\n" +
        "  \"dimensions\": [\"product\"],\n" +
        "  \"aggregations\": [\n" +
        "    {\n" +
        "      \"type\": \"" + aggType + "\",\n" +
        "      \"name\": \"" + groupByField + "\",\n" +
        "      \"fieldName\": \"" + aggField + "\"\n" +
        "    }\n" +
        "  ],\n" +
        "  \"intervals\": [\n" +
        "    \"2011-01-01T00:00:00.000Z/2011-05-01T00:00:00.000Z\"\n" +
        "  ]\n" +
        "}";
    ZipFile zip = new ZipFile(new File(this.getClass().getClassLoader().getResource("druid.sample.tsv.zip").toURI()));
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        zip.getInputStream(zip.getEntry("druid.sample.tsv")),
        recordParser,
        aggregator,
        0,
        Granularities.MONTH,
        100,
        groupBy
    );

    int groupByFieldNumber = ((GroupByQuery) helper.readQuery(groupBy)).getResultRowSignature()
                                                                       .indexOf(groupByField);

    List<ResultRow> results = seq.toList();
    Assert.assertEquals(36, results.size());
    Assert.assertEquals(expected, results.get(0).get(groupByFieldNumber));
  }
}
