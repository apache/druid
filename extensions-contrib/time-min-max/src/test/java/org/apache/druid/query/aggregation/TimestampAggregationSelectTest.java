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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.Resources;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Result;
import org.apache.druid.query.select.SelectResultValue;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.List;
import java.util.zip.ZipFile;

@RunWith(Parameterized.class)
public class TimestampAggregationSelectTest
{
  private AggregationTestHelper helper;

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private ColumnSelectorFactory selectorFactory;
  private TestObjectColumnSelector selector;

  private Timestamp[] values = new Timestamp[10];

  @Parameterized.Parameters(name = "{index}: Test for {0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return Iterables.transform(
        ImmutableList.of(
            ImmutableList.of("timeMin", "tmin", DateTimes.of("2011-01-12T01:00:00.000Z").getMillis()),
            ImmutableList.of("timeMax", "tmax", DateTimes.of("2011-01-31T01:00:00.000Z").getMillis())
        ),
        new Function<List<?>, Object[]>()
        {
          @Nullable
          @Override
          public Object[] apply(List<?> input)
          {
            return input.toArray();
          }
        }
    );
  }

  private String aggType;
  private String aggField;
  private Long expected;

  public TimestampAggregationSelectTest(String aggType, String aggField, Long expected)
  {
    this.aggType = aggType;
    this.aggField = aggField;
    this.expected = expected;
  }

  @Before
  public void setup()
  {
    helper = AggregationTestHelper.createSelectQueryAggregationTestHelper(
        new TimestampMinMaxModule().getJacksonModules(),
        temporaryFolder
    );

    selector = new TestObjectColumnSelector<>(values);
    selectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(selectorFactory.makeColumnValueSelector("test")).andReturn(selector);
    EasyMock.replay(selectorFactory);

  }

  @Test
  public void testSimpleDataIngestionAndSelectTest() throws Exception
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
    ZipFile zip = new ZipFile(new File(this.getClass().getClassLoader().getResource("druid.sample.tsv.zip").toURI()));
    Sequence<?> seq = helper.createIndexAndRunQueryOnSegment(
        zip.getInputStream(zip.getEntry("druid.sample.tsv")),
        recordParser,
        aggregator,
        0,
        Granularities.MONTH,
        100,
        Resources.toString(Resources.getResource("select.json"), StandardCharsets.UTF_8)
    );

    Result<SelectResultValue> result = (Result<SelectResultValue>) Iterables.getOnlyElement(seq.toList());
    Assert.assertEquals(36, result.getValue().getEvents().size());
    Assert.assertEquals(expected, result.getValue().getEvents().get(0).getEvent().get(aggField));
  }
}
