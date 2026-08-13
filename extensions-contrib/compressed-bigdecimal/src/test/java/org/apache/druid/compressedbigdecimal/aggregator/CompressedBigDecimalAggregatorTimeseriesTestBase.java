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

import com.google.common.collect.Iterables;
import org.apache.druid.compressedbigdecimal.ArrayCompressedBigDecimal;
import org.apache.druid.compressedbigdecimal.CompressedBigDecimalModule;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class CompressedBigDecimalAggregatorTimeseriesTestBase extends InitializedNullHandlingTest
{
  static final InputRowSchema SCHEMA = new InputRowSchema(
      new TimestampSpec("timestamp", "yyyyMMdd", null),
      DimensionsSpec.builder()
                    .setDimensions(
                        List.of(
                            new StringDimensionSchema("property"),
                            new StringDimensionSchema("revenue"),
                            new LongDimensionSchema("longRevenue"),
                            new DoubleDimensionSchema("doubleRevenue")
                        )
                    )
                    .build(),
      ColumnsFilter.all()
  );

  static final InputFormat FORMAT = new CsvInputFormat(
      List.of(
          "timestamp",
          "property",
          "revenue",
          "longRevenue",
          "doubleRevenue"
      ),
      null,
      null,
      null,
      0,
      null
  );

  private AggregationTestHelper helper;

  @TempDir
  public File tempFolder;

  @BeforeEach
  public void setUp()
  {
    final CompressedBigDecimalModule module = new CompressedBigDecimalModule();
    CompressedBigDecimalModule.registerSerde();
    helper = AggregationTestHelper.createTimeseriesQueryAggregationTestHelperWithTempDir(
        module.getJacksonModules(), tempFolder
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


  @Test
  public abstract void testIngestAndTimeseriesQuery() throws Exception;

  protected void testIngestAndTimeseriesQueryHelper(
      List<AggregatorFactory> ingestionAggregators,
      TimeseriesQuery query,
      String expected
  ) throws Exception
  {
    Sequence seq = helper.createIndexAndRunQueryOnSegment(
        this.getClass().getResourceAsStream("/" + "bd_test_data.csv"),
        SCHEMA,
        FORMAT,
        ingestionAggregators,
        0,
        Granularities.NONE,
        5,
        query
    );

    TimeseriesResultValue result = ((Result<TimeseriesResultValue>) Iterables.getOnlyElement(seq.toList())).getValue();
    Map<String, Object> event = result.getBaseObject();
    assertEquals(
        new DateTime("2017-01-01T00:00:00Z", DateTimeZone.forTimeZone(TimeZone.getTimeZone("UTC"))),
        ((Result<TimeseriesResultValue>) Iterables.getOnlyElement(seq.toList())).getTimestamp()
    );
    assertEquals(1, event.size());
    assertEquals(
        new ArrayCompressedBigDecimal(new BigDecimal(expected)),
        event.get("cbdStringRevenue")
    );
  }

  /**
   * Test using multiple segments.
   *
   * @throws Exception an exception
   */
  @Test
  public abstract void testIngestMultipleSegmentsAndTimeseriesQuery() throws Exception;

  protected void testIngestMultipleSegmentsAndTimeseriesQueryHelper(
      List<AggregatorFactory> ingestionAggregators,
      TimeseriesQuery query,
      String expected
  ) throws Exception
  {
    final File segmentDir1 = new File(tempFolder, "segment1");
    FileUtils.mkdirp(segmentDir1);
    helper.createIndex(
        new File(this.getClass().getResource("/" + "bd_test_data.csv").getFile()),
        SCHEMA,
        FORMAT,
        ingestionAggregators,
        segmentDir1,
        0,
        Granularities.NONE,
        5
    );
    final File segmentDir2 = new File(tempFolder, "segment2");
    FileUtils.mkdirp(segmentDir2);
    helper.createIndex(
        new File(this.getClass().getResource("/" + "bd_test_zero_data.csv").getFile()),
        SCHEMA,
        FORMAT,
        ingestionAggregators,
        segmentDir2,
        0,
        Granularities.NONE,
        5
    );

    Sequence seq = helper.runQueryOnSegments(
        Arrays.asList(segmentDir1, segmentDir2),
        query
    );

    TimeseriesResultValue result = ((Result<TimeseriesResultValue>) Iterables.getOnlyElement(seq.toList())).getValue();
    Map<String, Object> event = result.getBaseObject();
    assertEquals(
        new DateTime("2017-01-01T00:00:00Z", DateTimeZone.forTimeZone(TimeZone.getTimeZone("UTC"))),
        ((Result<TimeseriesResultValue>) Iterables.getOnlyElement(seq.toList())).getTimestamp()
    );
    assertEquals(1, event.size());
    assertEquals(
        new ArrayCompressedBigDecimal(new BigDecimal(expected)),
        event.get("cbdStringRevenue")
    );
  }
}
