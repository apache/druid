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

import com.google.common.collect.Iterables;
import com.google.common.io.Resources;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.TimeZone;

import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.hamcrest.collection.IsMapWithSize.aMapWithSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public abstract class CompressedBigDecimalAggregatorTimeseriesTestBase
{
  private final AggregationTestHelper helper;

  static {
    NullHandling.initializeForTests();
  }

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder(new File("target"));

  /**
   * Constructor.
   * *
   */
  public CompressedBigDecimalAggregatorTimeseriesTestBase()
  {
    CompressedBigDecimalModule module = new CompressedBigDecimalModule();
    CompressedBigDecimalModule.registerSerde();
    helper = AggregationTestHelper.createTimeseriesQueryAggregationTestHelper(
        module.getJacksonModules(), tempFolder);
  }

  /**
   * Default setup of UTC timezone.
   */
  @BeforeClass
  public static void setupClass()
  {
    System.setProperty("user.timezone", "UTC");
  }


  @Test
  public abstract void testIngestAndTimeseriesQuery() throws Exception;

  protected void testIngestAndTimeseriesQueryHelper(
      String jsonAggregatorsFile,
      String jsonQueryFile,
      String expected
  ) throws Exception
  {
    Sequence seq = helper.createIndexAndRunQueryOnSegment(
        this.getClass().getResourceAsStream("/" + "bd_test_data.csv"),
        Resources.asCharSource(
            getClass().getResource(
                "/" + "bd_test_data_parser.json"),
            StandardCharsets.UTF_8
        ).read(),
        Resources.asCharSource(
            this.getClass().getResource("/" + jsonAggregatorsFile),
            StandardCharsets.UTF_8
        ).read(),
        0,
        Granularities.NONE,
        5,
        Resources.asCharSource(
            this.getClass().getResource("/" + jsonQueryFile),
            StandardCharsets.UTF_8
        ).read()
    );

    TimeseriesResultValue result = ((Result<TimeseriesResultValue>) Iterables.getOnlyElement(seq.toList())).getValue();
    Map<String, Object> event = result.getBaseObject();
    assertEquals(
        new DateTime("2017-01-01T00:00:00Z", DateTimeZone.forTimeZone(TimeZone.getTimeZone("UTC"))),
        ((Result<TimeseriesResultValue>) Iterables.getOnlyElement(seq.toList())).getTimestamp()
    );
    assertThat(event, aMapWithSize(1));
    assertThat(
        event,
        hasEntry("cbdStringRevenue", new ArrayCompressedBigDecimal(new BigDecimal(expected)))
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
      String jsonAggregatorsFile,
      String jsonQueryFile,
      String expected
  ) throws Exception
  {
    File segmentDir1 = tempFolder.newFolder();
    helper.createIndex(
        new File(this.getClass().getResource("/" + "bd_test_data.csv").getFile()),
        Resources.asCharSource(
            this.getClass().getResource("/" + "bd_test_data_parser.json"),
            StandardCharsets.UTF_8
        ).read(),
        Resources.asCharSource(
            this.getClass().getResource("/" + jsonAggregatorsFile),
            StandardCharsets.UTF_8
        ).read(),
        segmentDir1,
        0,
        Granularities.NONE,
        5
    );
    File segmentDir2 = tempFolder.newFolder();
    helper.createIndex(
        new File(this.getClass().getResource("/" + "bd_test_zero_data.csv").getFile()),
        Resources.asCharSource(
            this.getClass().getResource("/" + "bd_test_data_parser.json"),
            StandardCharsets.UTF_8
        ).read(),
        Resources.asCharSource(
            this.getClass().getResource("/" + jsonAggregatorsFile),
            StandardCharsets.UTF_8
        ).read(),
        segmentDir2,
        0,
        Granularities.NONE,
        5
    );

    Sequence seq = helper.runQueryOnSegments(
        Arrays.asList(segmentDir1, segmentDir2),
        Resources.asCharSource(
            this.getClass().getResource("/" + jsonQueryFile),
            StandardCharsets.UTF_8
        ).read()
    );

    TimeseriesResultValue result = ((Result<TimeseriesResultValue>) Iterables.getOnlyElement(seq.toList())).getValue();
    Map<String, Object> event = result.getBaseObject();
    assertEquals(
        new DateTime("2017-01-01T00:00:00Z", DateTimeZone.forTimeZone(TimeZone.getTimeZone("UTC"))),
        ((Result<TimeseriesResultValue>) Iterables.getOnlyElement(seq.toList())).getTimestamp()
    );
    assertThat(event, aMapWithSize(1));
    assertThat(
        event,
        hasEntry("cbdStringRevenue", new ArrayCompressedBigDecimal(new BigDecimal(expected)))
    );

  }
}

