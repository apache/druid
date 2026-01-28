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

package org.apache.druid.indexing.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.impl.NoopInputSource;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.indexing.DataSchema;
import org.junit.Assert;
import org.junit.Test;

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
    final IndexTask.IndexIOConfig ioConfig = new IndexTask.IndexIOConfig(new NoopInputSource(), null, null, null);
    final Task task = new IndexTask(
        "theid",
        new TaskResource("rofl", 2),
        new IndexTask.IndexIngestionSpec(
            DataSchema.builder().withDataSource("foo").withObjectMapper(new DefaultObjectMapper()).build(),
            ioConfig,
            null
        ),
        null
    );
    final TaskStatus status = TaskStatus.running(task.getId());
    final TaskAnnouncement announcement = TaskAnnouncement.create(task, status, TaskLocation.unknown());

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
