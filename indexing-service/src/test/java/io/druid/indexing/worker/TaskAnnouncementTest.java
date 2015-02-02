/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.indexing.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.indexing.common.TaskStatus;
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
import junit.framework.Assert;
import org.junit.Test;

import java.io.File;

public class TaskAnnouncementTest
{
  @Test
  public void testBackwardsCompatibleSerde() throws Exception
  {
    final Task task = new RealtimeIndexTask(
        "theid",
        new TaskResource("rofl", 2),
        new FireDepartment(
            new DataSchema("foo", null, new AggregatorFactory[0], null),
            new RealtimeIOConfig(new LocalFirehoseFactory(new File("lol"), "rofl", null), new PlumberSchool()
            {
              @Override
              public Plumber findPlumber(
                  DataSchema schema, RealtimeTuningConfig config, FireDepartmentMetrics metrics
              )
              {
                return null;
              }
            }), null
        )
    );
    final TaskStatus status = TaskStatus.running(task.getId());
    final TaskAnnouncement announcement = TaskAnnouncement.create(task, status);

    final ObjectMapper jsonMapper = new DefaultObjectMapper();
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
