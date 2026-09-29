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

package org.apache.druid.compressedbigdecimal.aggregator;

import org.apache.druid.compressedbigdecimal.ArrayCompressedBigDecimal;
import org.apache.druid.compressedbigdecimal.CompressedBigDecimalGroupByQueryConfig;
import org.apache.druid.compressedbigdecimal.CompressedBigDecimalModule;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.ResultRow;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;


public abstract class CompressedBigDecimalAggregatorGroupByTestBase
{
  private final GroupByQueryConfig config;
  private final CompressedBigDecimalGroupByQueryConfig cbdGroupByQueryConfig;
  private AggregationTestHelper helper;

  @TempDir
  public File tempFolder;

  protected CompressedBigDecimalAggregatorGroupByTestBase(
      GroupByQueryConfig config,
      CompressedBigDecimalGroupByQueryConfig cbdGroupByQueryConfig
  )
  {
    this.config = config;
    this.cbdGroupByQueryConfig = cbdGroupByQueryConfig;
  }

  @BeforeEach
  public void setup()
  {
    final CompressedBigDecimalModule module = new CompressedBigDecimalModule();
    CompressedBigDecimalModule.registerSerde();
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        module.getJacksonModules(),
        config,
        tempFolder
    );
  }


  /**
   * Default setup of UTC timezone.
   */
  @BeforeAll
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
  public void testIngestAndGroupByAllQuery() throws Exception
  {
    final Sequence<ResultRow> seq;
    try (final InputStream inputStream = Objects.requireNonNull(
        CompressedBigDecimalAggregatorGroupByTestBase.class.getResourceAsStream("/bd_test_data.csv"),
        "Missing resource /bd_test_data.csv"
    )) {
      seq = helper.createIndexAndRunQueryOnSegment(
          inputStream,
          CompressedBigDecimalAggregatorTimeseriesTestBase.SCHEMA,
          CompressedBigDecimalAggregatorTimeseriesTestBase.FORMAT,
          cbdGroupByQueryConfig.getIngestionAggregators(),
          0,
          Granularities.NONE,
          5,
          cbdGroupByQueryConfig.getQuery()
      );
    }

    List<ResultRow> results = seq.toList();
    Assertions.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    MapBasedRow mapBasedRow = row.toMapBasedRow(cbdGroupByQueryConfig.getQuery());
    Map<String, Object> event = mapBasedRow.getEvent();
    Assertions.assertEquals(
        new DateTime("2017-01-01T00:00:00Z", DateTimeZone.forTimeZone(TimeZone.getTimeZone("UTC"))),
        mapBasedRow.getTimestamp()
    );
    Assertions.assertEquals(3, event.size());
    Assertions.assertEquals(
        new ArrayCompressedBigDecimal(new BigDecimal(cbdGroupByQueryConfig.getStringRevenue())),
        event.get("cbdRevenueFromString")
    );
    // long conversion of 5000000000.000000005 results in null/0 value
    Assertions.assertEquals(
        new ArrayCompressedBigDecimal(new BigDecimal(cbdGroupByQueryConfig.getLongRevenue())),
        event.get("cbdRevenueFromLong")
    );
    // double input changes 5000000000.000000005 to 5000000000.5 to fit in double mantissa space
    Assertions.assertEquals(
        new ArrayCompressedBigDecimal(new BigDecimal(cbdGroupByQueryConfig.getDoubleRevenue())),
        event.get("cbdRevenueFromDouble")
    );
  }
}
