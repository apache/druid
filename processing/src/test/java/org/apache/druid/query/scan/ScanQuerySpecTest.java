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

package org.apache.druid.query.scan;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.spec.LegacySegmentSpec;
import org.apache.druid.segment.VirtualColumns;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class ScanQuerySpecTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  @Test
  public void testSerializationLegacyString() throws Exception
  {
    String legacy =
        "{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"table\",\"name\":\"testing\"},"
        + "\"intervals\":{\"type\":\"LegacySegmentSpec\",\"intervals\":[\"2011-01-12T00:00:00.000Z/2011-01-14T00:00:00.000Z\"]},"
        + "\"filter\":null,"
        + "\"columns\":[\"market\",\"quality\",\"index\"],"
        + "\"limit\":3,"
        + "\"context\":null}";

    String current =
        "{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"table\",\"name\":\"testing\"},"
        + "\"intervals\":{\"type\":\"LegacySegmentSpec\",\"intervals\":[\"2011-01-12T00:00:00.000Z/2011-01-14T00:00:00.000Z\"]},"
        + "\"virtualColumns\":[],"
        + "\"resultFormat\":\"list\","
        + "\"batchSize\":20480,"
        + "\"limit\":3,"
        + "\"order\":\"none\","
        + "\"filter\":null,"
        + "\"columns\":[\"market\",\"quality\",\"index\"],"
        + "\"legacy\":null,"
        + "\"context\":null,"
        + "\"descending\":false,"
        + "\"granularity\":{\"type\":\"all\"}}";

    ScanQuery query = new ScanQuery(
        new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE),
        new LegacySegmentSpec(Intervals.of("2011-01-12/2011-01-14")),
        VirtualColumns.EMPTY,
        ScanQuery.ResultFormat.RESULT_FORMAT_LIST,
        0,
        3,
        ScanQuery.Order.NONE,
        null,
        Arrays.asList("market", "quality", "index"),
        null,
        null
    );

    String actual = JSON_MAPPER.writeValueAsString(query);
    Assert.assertEquals(current, actual);
    Assert.assertEquals(query, JSON_MAPPER.readValue(actual, ScanQuery.class));
    Assert.assertEquals(query, JSON_MAPPER.readValue(legacy, ScanQuery.class));
  }
}
