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

package org.apache.druid.indexing.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;

public class TaskLockTest
{
  private final ObjectMapper objectMapper = new DefaultObjectMapper();

  @Test
  public void testSerdeTimeChunkLock() throws IOException
  {
    final TimeChunkLock lock = new TimeChunkLock(
        TaskLockType.EXCLUSIVE,
        "groupId",
        "dataSource",
        Intervals.of("2019/2020"),
        "version",
        100
    );
    final String json = objectMapper.writeValueAsString(lock);
    final TaskLock fromJson = objectMapper.readValue(json, TaskLock.class);

    Assert.assertEquals(lock, fromJson);
  }

  @Test
  public void testDeserializeTimeChunkLockWithoutType() throws IOException
  {
    final TimeChunkLock expected = new TimeChunkLock(
        TaskLockType.EXCLUSIVE,
        "groupId",
        "dataSource",
        Intervals.of("2019/2020"),
        "version",
        100
    );

    final String json = "{\n"
                        + "  \"type\" : \"EXCLUSIVE\",\n"
                        + "  \"groupId\" : \"groupId\",\n"
                        + "  \"dataSource\" : \"dataSource\",\n"
                        + "  \"interval\" : \"2019-01-01T00:00:00.000Z/2020-01-01T00:00:00.000Z\",\n"
                        + "  \"version\" : \"version\",\n"
                        + "  \"priority\" : 100,\n"
                        + "  \"revoked\" : false\n"
                        + "}";

    Assert.assertEquals(expected, objectMapper.readValue(json, TaskLock.class));
  }

  @Test
  public void testSerdeSegmentLock() throws IOException
  {
    final SegmentLock lock = new SegmentLock(
        TaskLockType.EXCLUSIVE,
        "groupId",
        "dataSource",
        Intervals.of("2019/2020"),
        "version",
        0,
        100
    );
    final String json = objectMapper.writeValueAsString(lock);
    final TaskLock fromJson = objectMapper.readValue(json, TaskLock.class);

    Assert.assertEquals(lock, fromJson);
  }

  @Test
  public void testSerdeOldLock() throws IOException
  {
    final OldTaskLock oldTaskLock = new OldTaskLock(
        TaskLockType.EXCLUSIVE,
        "groupId",
        "dataSource",
        Intervals.of(("2019/2020")),
        "version",
        10,
        true
    );
    final byte[] json = objectMapper.writeValueAsBytes(oldTaskLock);
    final TaskLock fromJson = objectMapper.readValue(json, TaskLock.class);
    Assert.assertEquals(LockGranularity.TIME_CHUNK, fromJson.getGranularity());
    Assert.assertEquals(TaskLockType.EXCLUSIVE, fromJson.getType());
    Assert.assertEquals("groupId", fromJson.getGroupId());
    Assert.assertEquals("dataSource", fromJson.getDataSource());
    Assert.assertEquals(Intervals.of("2019/2020"), fromJson.getInterval());
    Assert.assertEquals("version", fromJson.getVersion());
    Assert.assertEquals(10, fromJson.getPriority().intValue());
    Assert.assertTrue(fromJson.isRevoked());
  }

  private static class OldTaskLock
  {
    private final TaskLockType type;
    private final String groupId;
    private final String dataSource;
    private final Interval interval;
    private final String version;
    private final Integer priority;
    private final boolean revoked;

    @JsonCreator
    public OldTaskLock(
        @JsonProperty("type") @Nullable TaskLockType type,
        @JsonProperty("groupId") String groupId,
        @JsonProperty("dataSource") String dataSource,
        @JsonProperty("interval") Interval interval,
        @JsonProperty("version") String version,
        @JsonProperty("priority") @Nullable Integer priority,
        @JsonProperty("revoked") boolean revoked
    )
    {
      this.type = type == null ? TaskLockType.EXCLUSIVE : type;
      this.groupId = Preconditions.checkNotNull(groupId, "groupId");
      this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
      this.interval = Preconditions.checkNotNull(interval, "interval");
      this.version = Preconditions.checkNotNull(version, "version");
      this.priority = priority;
      this.revoked = revoked;
    }

    @JsonProperty
    public TaskLockType getType()
    {
      return type;
    }

    @JsonProperty
    public String getGroupId()
    {
      return groupId;
    }

    @JsonProperty
    public String getDataSource()
    {
      return dataSource;
    }

    @JsonProperty
    public Interval getInterval()
    {
      return interval;
    }

    @JsonProperty
    public String getVersion()
    {
      return version;
    }

    @JsonProperty
    @Nullable
    public Integer getPriority()
    {
      return priority;
    }

    @JsonProperty
    public boolean isRevoked()
    {
      return revoked;
    }

  }
}
