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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.junit.Assert;
import org.junit.Test;

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
                        + "  \"lockType\" : \"EXCLUSIVE\",\n"
                        + "  \"groupId\" : \"groupId\",\n"
                        + "  \"dataSource\" : \"dataSource\",\n"
                        + "  \"interval\" : \"2019-01-01T00:00:00.000Z/2020-01-01T00:00:00.000Z\",\n"
                        + "  \"version\" : \"version\",\n"
                        + "  \"priority\" : 100,\n"
                        + "  \"revoked\" : false,\n"
                        + "  \"type\" : \"timeChunk\"\n"
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
}
