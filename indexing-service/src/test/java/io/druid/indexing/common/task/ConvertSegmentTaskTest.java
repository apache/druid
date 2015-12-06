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

package io.druid.indexing.common.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.indexing.common.TestUtils;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class ConvertSegmentTaskTest
{
  private final ObjectMapper jsonMapper;

  public ConvertSegmentTaskTest()
  {
    TestUtils testUtils = new TestUtils();
    jsonMapper = testUtils.getTestObjectMapper();
  }

  @Test
  public void testSerializationSimple() throws Exception
  {
    final String dataSource = "billy";
    final Interval interval = new Interval(new DateTime().minus(1000), new DateTime());

    ConvertSegmentTask task = ConvertSegmentTask.create(dataSource, interval, null, false, true, null);

    Task task2 = jsonMapper.readValue(jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(task), Task.class);
    Assert.assertEquals(task, task2);

    DataSegment segment = new DataSegment(
        dataSource,
        interval,
        new DateTime().toString(),
        ImmutableMap.<String, Object>of(),
        ImmutableList.<String>of(),
        ImmutableList.<String>of(),
        new NoneShardSpec(),
        9,
        102937
    );

    task = ConvertSegmentTask.create(segment, null, false, true, null);

    task2 = jsonMapper.readValue(jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(task), Task.class);
    Assert.assertEquals(task, task2);
  }

  @Test
  public void testdeSerializationFromJsonString() throws Exception
  {
    String json = "{\n"
                  + "  \"type\" : \"convert_segment\",\n"
                  + "  \"dataSource\" : \"billy\",\n"
                  + "  \"interval\" : \"2015-08-27T00:00:00.000Z/2015-08-28T00:00:00.000Z\"\n"
                  + "}";
    ConvertSegmentTask task = (ConvertSegmentTask) jsonMapper.readValue(json, Task.class);
    Assert.assertEquals("billy", task.getDataSource());
    Assert.assertEquals(new Interval("2015-08-27T00:00:00.000Z/2015-08-28T00:00:00.000Z"), task.getInterval());
  }

  @Test
  public void testSerdeBackwardsCompatible() throws Exception
  {
    String json = "{\n"
                  + "  \"type\" : \"version_converter\",\n"
                  + "  \"dataSource\" : \"billy\",\n"
                  + "  \"interval\" : \"2015-08-27T00:00:00.000Z/2015-08-28T00:00:00.000Z\"\n"
                  + "}";
    ConvertSegmentTask task = (ConvertSegmentTask) jsonMapper.readValue(json, Task.class);
    Assert.assertEquals("billy", task.getDataSource());
    Assert.assertEquals(new Interval("2015-08-27T00:00:00.000Z/2015-08-28T00:00:00.000Z"), task.getInterval());
  }
}
