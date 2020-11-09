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

package org.apache.druid.compressedbigdecimal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.hamcrest.collection.IsMapWithSize.aMapWithSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Unit tests for AccumulatingDecimalAggregator.
 */
@RunWith(Parameterized.class)
public class CompressedBigDecimalAggregatorGroupByTest
{
  private final AggregationTestHelper helper;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder(new File("target"));

  /**
   * Constructor.
   *
   * @param config config object
   */
  public CompressedBigDecimalAggregatorGroupByTest(GroupByQueryConfig config)
  {
    CompressedBigDecimalModule module = new CompressedBigDecimalModule();
    module.configure(null);
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        module.getJacksonModules(), config, tempFolder);
  }

  /**
   * Constructor feeder.
   *
   * @return constructors
   */
  @Parameterized.Parameters(name = "{0}")
  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
      constructors.add(new Object[] {config});
    }
    return constructors;
  }

  /**
   * Default setup of UTC timezone.
   */
  @BeforeClass
  public static void setupClass()
  {
    System.setProperty("user.timezone", "UTC");
  }

  /**
   * ingetion method for all groupBy query.
   *
   * @throws IOException IOException
   * @throws Exception   Exception
   */
  @Test
  public void testIngestAndGroupByAllQuery() throws IOException, Exception
  {

    String groupByQueryJson = Resources.asCharSource(
        this.getClass().getResource("/" + "bd_test_groupby_query.json"),
        StandardCharsets.UTF_8
    ).read();

    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        this.getClass().getResourceAsStream("/" + "bd_test_data.csv"),
        Resources.asCharSource(this.getClass().getResource(
            "/" + "bd_test_data_parser.json"),
            StandardCharsets.UTF_8
        ).read(),
        Resources.asCharSource(
            this.getClass().getResource("/" + "bd_test_aggregators.json"),
            StandardCharsets.UTF_8
        ).read(),
        0,
        Granularities.NONE,
        5,
        groupByQueryJson
    );

    List<ResultRow> results = seq.toList();
    ResultRow row = results.get(0);
    assertThat(results, hasSize(1));
    ObjectMapper mapper = helper.getObjectMapper();
    GroupByQuery groupByQuery = mapper.readValue(groupByQueryJson, GroupByQuery.class);
    MapBasedRow mapBasedRow = row.toMapBasedRow(groupByQuery);
    Map<String, Object> event = mapBasedRow.getEvent();
    assertEquals(new DateTime("2017-01-01T00:00:00Z", DateTimeZone.forTimeZone(TimeZone.getTimeZone("UTC"))), mapBasedRow.getTimestamp());
    assertThat(event, aMapWithSize(1));
    assertThat(event, hasEntry("revenue", new BigDecimal("15000000010.000000005")));
  }
}
