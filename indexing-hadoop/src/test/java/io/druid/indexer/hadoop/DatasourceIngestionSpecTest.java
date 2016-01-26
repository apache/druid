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

package io.druid.indexer.hadoop;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.granularity.QueryGranularity;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.segment.TestHelper;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 */
public class DatasourceIngestionSpecTest
{
  private static final ObjectMapper MAPPER = TestHelper.getObjectMapper();

  @Test
  public void testSingleIntervalSerde() throws Exception
  {
    Interval interval = Interval.parse("2014/2015");

    DatasourceIngestionSpec expected = new DatasourceIngestionSpec(
        "test",
        interval,
        null,
        new SelectorDimFilter("dim", "value"),
        QueryGranularity.DAY,
        Lists.newArrayList("d1", "d2"),
        Lists.newArrayList("m1", "m2", "m3"),
        false
    );

    DatasourceIngestionSpec actual = MAPPER.readValue(MAPPER.writeValueAsString(expected), DatasourceIngestionSpec.class);
    Assert.assertEquals(ImmutableList.of(interval), actual.getIntervals());
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testMultiIntervalSerde() throws Exception
  {
    //defaults
    String jsonStr = "{\n"
                     + "  \"dataSource\": \"test\",\n"
                     + "  \"intervals\": [\"2014/2015\", \"2016/2017\"]\n"
                     + "}\n";

    DatasourceIngestionSpec actual = MAPPER.readValue(
        MAPPER.writeValueAsString(
            MAPPER.readValue(jsonStr, DatasourceIngestionSpec.class)
        ),
        DatasourceIngestionSpec.class
    );

    List<Interval> intervals = ImmutableList.of(Interval.parse("2014/2015"), Interval.parse("2016/2017"));

    DatasourceIngestionSpec expected = new DatasourceIngestionSpec(
        "test",
        null,
        intervals,
        null,
        null,
        null,
        null,
        false
    );

    Assert.assertEquals(expected, actual);

    //non-defaults
    jsonStr = "{\n"
              + "  \"dataSource\": \"test\",\n"
              + "  \"intervals\": [\"2014/2015\", \"2016/2017\"],\n"
              + "  \"filter\": { \"type\": \"selector\", \"dimension\": \"dim\", \"value\": \"value\"},\n"
              + "  \"granularity\": \"day\",\n"
              + "  \"dimensions\": [\"d1\", \"d2\"],\n"
              + "  \"metrics\": [\"m1\", \"m2\", \"m3\"],\n"
              + "  \"ignoreWhenNoSegments\": true\n"
              + "}\n";

    expected = new DatasourceIngestionSpec(
        "test",
        null,
        intervals,
        new SelectorDimFilter("dim", "value"),
        QueryGranularity.DAY,
        Lists.newArrayList("d1", "d2"),
        Lists.newArrayList("m1", "m2", "m3"),
        true
    );

    actual = MAPPER.readValue(
        MAPPER.writeValueAsString(
            MAPPER.readValue(jsonStr, DatasourceIngestionSpec.class)
        ),
        DatasourceIngestionSpec.class
    );

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testOldJsonDeserialization() throws Exception
  {
    String jsonStr = "{\"dataSource\": \"test\", \"interval\": \"2014/2015\"}";
    DatasourceIngestionSpec actual = MAPPER.readValue(jsonStr, DatasourceIngestionSpec.class);

    Assert.assertEquals(
        new DatasourceIngestionSpec("test", Interval.parse("2014/2015"), null, null, null, null, null, false),
        actual
    );
  }
}
