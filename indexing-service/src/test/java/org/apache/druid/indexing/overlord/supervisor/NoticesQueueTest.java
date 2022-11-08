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

package org.apache.druid.indexing.overlord.supervisor;

import org.apache.druid.indexing.seekablestream.supervisor.NoticesQueue;
import org.junit.Assert;
import org.junit.Test;

public class NoticesQueueTest
{
  @Test
  public void testQueue() throws InterruptedException
  {
    final NoticesQueue<String> queue = new NoticesQueue<>();

    for (int i = 0 ; i < 3; i++) {
      Assert.assertEquals(0, queue.size());
      queue.add("xyz");
      Assert.assertEquals(1, queue.size());

      queue.add("xyz");
      Assert.assertEquals(1, queue.size());

      queue.add("foo");
      Assert.assertEquals(2, queue.size());

      queue.add("xyz");
      Assert.assertEquals(2, queue.size());

      queue.add("bar");
      Assert.assertEquals(3, queue.size());

      Assert.assertEquals("xyz", queue.poll(10));
      Assert.assertEquals("foo", queue.poll(10));
      Assert.assertEquals("bar", queue.poll(10));
      Assert.assertNull(queue.poll(10));
      Assert.assertEquals(0, queue.size());
    }
  }
}
