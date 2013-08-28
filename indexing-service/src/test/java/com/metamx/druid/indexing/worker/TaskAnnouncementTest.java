package com.metamx.druid.indexing.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.druid.index.v1.IndexGranularity;
import com.metamx.druid.indexing.common.TaskStatus;
import com.metamx.druid.indexing.common.task.RealtimeIndexTask;
import com.metamx.druid.indexing.common.task.Task;
import com.metamx.druid.indexing.common.task.TaskResource;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.realtime.Schema;
import com.metamx.druid.shard.NoneShardSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import junit.framework.Assert;
import org.joda.time.Period;
import org.junit.Test;

public class TaskAnnouncementTest
{
  @Test
  public void testBackwardsCompatibleSerde() throws Exception
  {
    final Task task = new RealtimeIndexTask(
        "theid",
        new TaskResource("rofl", 2),
        new Schema("foo", null, new AggregatorFactory[0], QueryGranularity.NONE, new NoneShardSpec()),
        null,
        null,
        new Period("PT10M"),
        IndexGranularity.HOUR,
        null
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
