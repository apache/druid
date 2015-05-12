/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexing.common.task;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.jackson.DefaultObjectMapper;
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
  @Test
  public void testSerializationSimple() throws Exception
  {
    final String dataSource = "billy";
    final Interval interval = new Interval(new DateTime().minus(1000), new DateTime());

    DefaultObjectMapper jsonMapper = new DefaultObjectMapper();

    ConvertSegmentTask task = ConvertSegmentTask.create(dataSource, interval, null, false, true);

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

    task = ConvertSegmentTask.create(segment, null, false, true);

    task2 = jsonMapper.readValue(jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(task), Task.class);
    Assert.assertEquals(task, task2);
  }

  @Test
  public void testDeserialization() throws Exception
  {
    DefaultObjectMapper jsonMapper = new DefaultObjectMapper();

    //this one deserializes successfully because "version_converter" is listed first on the Task interface
    ConvertSegmentTask task1 = jsonMapper.readValue(taskString("version_converter"), ConvertSegmentTask.class);

    //this one fails to deserialize with following error
    //com.fasterxml.jackson.databind.JsonMappingException: Could not resolve type id 'convert_segment' into a subtype of
    //[simple type, class io.druid.indexing.common.task.ConvertSegmentTask]
    ConvertSegmentTask task2 = jsonMapper.readValue(taskString("convert_segment"), ConvertSegmentTask.class);
  }

  private String taskString(String type)
  {
    return "{"
           +  "\"type\" : \"" + type + "\","
           +  "\"id\" : \"convert_segment_billy_2015-05-12T02:51:35.863Z_2015-05-12T02:51:36.943Z_2015-05-12T02:51:38.262Z\","
           +  "\"groupId\" : \"convert_segment_billy_2015-05-12T02:51:35.863Z_2015-05-12T02:51:36.943Z_2015-05-12T02:51:38.262Z\","
           +  "\"dataSource\" : \"billy\","
           +  "\"interval\" : \"2015-05-12T02:51:35.863Z/2015-05-12T02:51:36.943Z\","
           +  "\"segment\" : null,"
           +  "\"indexSpec\" : {"
           +  "  \"bitmap\" : {"
           +  "    \"type\" : \"concise\""
           +  "  },"
           +  "  \"dimensionCompression\" : null,"
           +  "  \"metricCompression\" : null"
           +  "},"
           +  "\"force\" : false,"
           +  "\"validate\" : true,"
           +  "\"resource\" : {"
           +  "  \"availabilityGroup\" : \"convert_segment_billy_2015-05-12T02:51:35.863Z_2015-05-12T02:51:36.943Z_2015-05-12T02:51:38.262Z\","
           +  "  \"requiredCapacity\" : 1"
           +"}"
           +"}";
  }
}
