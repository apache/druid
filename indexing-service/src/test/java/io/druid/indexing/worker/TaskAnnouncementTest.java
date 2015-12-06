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

package io.druid.indexing.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TestUtils;
import io.druid.indexing.common.task.RealtimeIndexTask;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.common.task.TaskResource;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeIOConfig;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.realtime.FireDepartment;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.firehose.LocalFirehoseFactory;
import io.druid.segment.realtime.plumber.Plumber;
import io.druid.segment.realtime.plumber.PlumberSchool;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class TaskAnnouncementTest
{
  private final ObjectMapper jsonMapper;

  public TaskAnnouncementTest()
  {
    TestUtils testUtils = new TestUtils();
    jsonMapper = testUtils.getTestObjectMapper();
  }

  @Test
  public void testBackwardsCompatibleSerde() throws Exception
  {
    final Task task = new RealtimeIndexTask(
        "theid",
        new TaskResource("rofl", 2),
        new FireDepartment(
            new DataSchema("foo", null, new AggregatorFactory[0], null, new DefaultObjectMapper()),
            new RealtimeIOConfig(
                new LocalFirehoseFactory(new File("lol"), "rofl", null), new PlumberSchool()
            {
              @Override
              public Plumber findPlumber(
                  DataSchema schema, RealtimeTuningConfig config, FireDepartmentMetrics metrics
              )
              {
                return null;
              }

            },
                null
            ),
            null
        ),
        null
    );
    final TaskStatus status = TaskStatus.running(task.getId());
    final TaskAnnouncement announcement = TaskAnnouncement.create(task, status);

    final String statusJson = jsonMapper.writeValueAsString(status);
    final String announcementJson = jsonMapper.writeValueAsString(announcement);

    final TaskStatus statusFromStatus = jsonMapper.readValue(statusJson, TaskStatus.class);
    final TaskStatus statusFromAnnouncement = jsonMapper.readValue(announcementJson, TaskStatus.class);
    final TaskAnnouncement announcementFromStatus = jsonMapper.readValue(statusJson, TaskAnnouncement.class);
    final TaskAnnouncement announcementFromAnnouncement = jsonMapper.readValue(
        announcementJson,
        TaskAnnouncement.class
    );

    Assert.assertEquals("theid", statusFromStatus.getId());
    Assert.assertEquals("theid", statusFromAnnouncement.getId());
    Assert.assertEquals("theid", announcementFromStatus.getTaskStatus().getId());
    Assert.assertEquals("theid", announcementFromAnnouncement.getTaskStatus().getId());

    Assert.assertEquals("theid", announcementFromStatus.getTaskResource().getAvailabilityGroup());
    Assert.assertEquals("rofl", announcementFromAnnouncement.getTaskResource().getAvailabilityGroup());

    Assert.assertEquals(1, announcementFromStatus.getTaskResource().getRequiredCapacity());
    Assert.assertEquals(2, announcementFromAnnouncement.getTaskResource().getRequiredCapacity());
  }
}
