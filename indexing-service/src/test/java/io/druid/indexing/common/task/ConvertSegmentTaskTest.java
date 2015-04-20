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

    ConvertSegmentTask task = ConvertSegmentTask.create(dataSource, interval, null, false);

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

    task = ConvertSegmentTask.create(segment, null, false);

    task2 = jsonMapper.readValue(jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(task), Task.class);
    Assert.assertEquals(task, task2);
  }
}
