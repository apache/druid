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

package io.druid.indexing.common.actions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.indexing.common.task.IngestionTestBase;
import io.druid.indexing.common.task.NoopTask;
import io.druid.indexing.common.task.Task;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CountingSegmentAllocateActionTest extends IngestionTestBase
{
  @Test
  public void testJsonSerde() throws IOException
  {
    final SimpleModule module = new SimpleModule();
    module.addSerializer(Interval.class, ToStringSerializer.instance);

    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.registerModule(module);
    mapper.registerSubtypes(new NamedType(NumberedShardSpec.class, "numbered"));

    final CountingSegmentAllocateAction action = createAction(DateTimes.nowUtc());

    final String json = mapper.writeValueAsString(action);
    Assert.assertEquals(action, mapper.readValue(json, TaskAction.class));
  }

  @Test
  public void testPerform()
  {
    final Task task = NoopTask.create();
    final TaskActionToolbox toolbox = createTaskActionToolbox();

    CountingSegmentAllocateAction action;
    SegmentIdentifier segmentIdentifier;

    for (int i = 0; i < 3; i++) {
      action = createAction(DateTimes.of("2017-01-01"));
      segmentIdentifier = action.perform(task, toolbox);
      Assert.assertEquals(
          StringUtils.format("dataSource_2017-01-01T00:00:00.000Z_2017-01-02T00:00:00.000Z_version1_%d", i + 1),
          segmentIdentifier.toString()
      );
    }

    for (int i = 0; i < 3; i++) {
      action = createAction(DateTimes.of("2017-01-02"));
      segmentIdentifier = action.perform(task, toolbox);
      Assert.assertEquals(
          StringUtils.format("dataSource_2017-01-02T00:00:00.000Z_2017-01-03T00:00:00.000Z_version1_%d", i + 1),
          segmentIdentifier.toString()
      );
    }
  }

  @Test
  public void testPerformParallel()
  {
    final List<Task> tasks = IntStream.range(0, 5).mapToObj(i -> NoopTask.create()).collect(Collectors.toList());
    final ExecutorService service = Execs.multiThreaded(5, "counting-segment-allocate-action-test-%d");
    final TaskActionToolbox toolbox = createTaskActionToolbox();

    try {
      final List<SegmentIdentifier> segmentIdentifiers = tasks
          .stream()
          .map(task -> {
            final CountingSegmentAllocateAction action = createAction(DateTimes.of("2017-01-01"));
            return action.perform(task, toolbox);
          })
          .sorted(Comparator.comparing(SegmentIdentifier::toString))
          .collect(Collectors.toList());
      for (int i = 0; i < 5; i++) {
        Assert.assertEquals(
            StringUtils.format("dataSource_2017-01-01T00:00:00.000Z_2017-01-02T00:00:00.000Z_version1_%d", i + 1),
            segmentIdentifiers.get(i).toString()
        );
      }
    }
    finally {
      service.shutdownNow();
    }
  }

  private CountingSegmentAllocateAction createAction(DateTime timestamp)
  {
    return new CountingSegmentAllocateAction(
        "dataSource",
        timestamp,
        new ArbitraryGranularitySpec(
            Granularities.DAY,
            ImmutableList.of(Intervals.of("2017-01-01/2017-01-02"), Intervals.of("2017-01-02/2017-01-03"))
        ),
        ImmutableMap.of(
            Intervals.of("2017-01-01/2017-01-02"),
            ImmutableList.of(new NumberedShardSpec(0, 0)),
            Intervals.of("2017-01-02/2017-01-03"),
            ImmutableList.of(new NumberedShardSpec(1, 0))
        ),
        ImmutableMap.of(
            Intervals.of("2017-01-01/2017-01-02"),
            "version1",
            Intervals.of("2017-01-02/2017-01-03"),
            "version1"
        )
    );
  }
}
