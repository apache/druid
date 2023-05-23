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

package org.apache.druid.tasklogs;

import com.google.common.base.Optional;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

public class TaskLogStreamerTest
{
  /**
   * Test default implemenation of streamTaskStatus in TaskLogStreamer interface for code coverage
   *
   * @throws IOException
   */
  @Test
  public void test_streamTaskStatus() throws IOException
  {
    TaskLogStreamer taskLogStreamer = new TaskLogStreamer() {
      @Override
      public Optional<InputStream> streamTaskLog(String taskid, long offset)
      {
        return Optional.absent();
      }
    };
    Assert.assertFalse(taskLogStreamer.streamTaskStatus("id").isPresent());
  }
}
