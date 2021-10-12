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

public class TimeChunkLockTryAcquireActionTest
{
  @Rule
  public TaskActionTestKit actionTestKit = new TaskActionTestKit();

  private final ObjectMapper mapper = new DefaultObjectMapper();

  @Test
  public void testSerdeWithAllFields() throws IOException
  {
    final TimeChunkLockTryAcquireAction expected = new TimeChunkLockTryAcquireAction(
        TaskLockType.SHARED,
        Intervals.of("2017-01-01/2017-01-02")
    );

    final byte[] bytes = mapper.writeValueAsBytes(expected);
    final TimeChunkLockTryAcquireAction actual = mapper.readValue(bytes, TimeChunkLockTryAcquireAction.class);
    Assert.assertEquals(expected.getType(), actual.getType());
    Assert.assertEquals(expected.getInterval(), actual.getInterval());
  }

  @Test
  public void testSerdeFromJsonWithMissingFields() throws IOException
  {
    final String json = "{ \"type\": \"lockTryAcquire\", \"interval\" : \"2017-01-01/2017-01-02\" }";

    final TimeChunkLockTryAcquireAction actual = mapper.readValue(json, TimeChunkLockTryAcquireAction.class);
    final TimeChunkLockTryAcquireAction expected = new TimeChunkLockTryAcquireAction(
        TaskLockType.EXCLUSIVE,
        Intervals.of("2017-01-01/2017-01-02")
    );
    Assert.assertEquals(expected.getType(), actual.getType());
    Assert.assertEquals(expected.getInterval(), actual.getInterval());
  }

  @Test(timeout = 60_000L)
  public void testWithLockType()
  {
    final Task task = NoopTask.create();
    final TimeChunkLockTryAcquireAction action = new TimeChunkLockTryAcquireAction(
        TaskLockType.EXCLUSIVE,
        Intervals.of("2017-01-01/2017-01-02")
    );

    actionTestKit.getTaskLockbox().add(task);
    final TaskLock lock = action.perform(task, actionTestKit.getTaskActionToolbox());
    Assert.assertNotNull(lock);
  }

  @Test(timeout = 60_000L)
  public void testWithoutLockType()
  {
    final Task task = NoopTask.create();
    final TimeChunkLockTryAcquireAction action = new TimeChunkLockTryAcquireAction(
        null,
        Intervals.of("2017-01-01/2017-01-02")
    );

    actionTestKit.getTaskLockbox().add(task);
    final TaskLock lock = action.perform(task, actionTestKit.getTaskActionToolbox());
    Assert.assertNotNull(lock);
  }
}
