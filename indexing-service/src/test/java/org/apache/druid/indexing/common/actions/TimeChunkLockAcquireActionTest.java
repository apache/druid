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

package org.apache.druid.indexing.common.actions;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

public class TimeChunkLockAcquireActionTest
{
  @Rule
  public TaskActionTestKit actionTestKit = new TaskActionTestKit();

  private final ObjectMapper mapper = new DefaultObjectMapper();

  @Test
  public void testSerdeWithAllFields() throws IOException
  {
    final TimeChunkLockAcquireAction expected = new TimeChunkLockAcquireAction(
        TaskLockType.SHARED,
        Intervals.of("2017-01-01/2017-01-02"),
        1000
    );

    final byte[] bytes = mapper.writeValueAsBytes(expected);
    final TimeChunkLockAcquireAction actual = mapper.readValue(bytes, TimeChunkLockAcquireAction.class);
    Assert.assertEquals(expected.getType(), actual.getType());
    Assert.assertEquals(expected.getInterval(), actual.getInterval());
    Assert.assertEquals(expected.getTimeoutMs(), actual.getTimeoutMs());
  }

  @Test
  public void testSerdeFromJsonWithMissingFields() throws IOException
  {
    final String json = "{ \"type\": \"lockAcquire\", \"interval\" : \"2017-01-01/2017-01-02\" }";

    final TimeChunkLockAcquireAction actual = mapper.readValue(json, TimeChunkLockAcquireAction.class);
    final TimeChunkLockAcquireAction expected = new TimeChunkLockAcquireAction(
        TaskLockType.EXCLUSIVE,
        Intervals.of("2017-01-01/2017-01-02"),
        0
    );
    Assert.assertEquals(expected.getType(), actual.getType());
    Assert.assertEquals(expected.getInterval(), actual.getInterval());
    Assert.assertEquals(expected.getTimeoutMs(), actual.getTimeoutMs());
  }

  @Test(timeout = 60_000L)
  public void testWithLockType()
  {
    final Task task = NoopTask.create();
    final TimeChunkLockAcquireAction action = new TimeChunkLockAcquireAction(
        TaskLockType.EXCLUSIVE,
        Intervals.of("2017-01-01/2017-01-02"),
        1000
    );

    actionTestKit.getTaskLockbox().add(task);
    final TaskLock lock = action.perform(task, actionTestKit.getTaskActionToolbox());
    Assert.assertNotNull(lock);
  }

  @Test(timeout = 60_000L)
  public void testWithoutLockType()
  {
    final Task task = NoopTask.create();
    final TimeChunkLockAcquireAction action = new TimeChunkLockAcquireAction(
        null,
        Intervals.of("2017-01-01/2017-01-02"),
        1000
    );

    actionTestKit.getTaskLockbox().add(task);
    final TaskLock lock = action.perform(task, actionTestKit.getTaskActionToolbox());
    Assert.assertNotNull(lock);
  }
}
