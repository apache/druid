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

package org.apache.druid.msq.dart.worker;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DartWorkerContextSegmentLoadAheadTest
{
  private static final int THREAD_COUNT = 8;

  @Test
  public void test_noWorkerConfig_noContext_fallsBackToTwiceThreadCount()
  {
    assertEquals(16, DartWorkerContext.resolveSegmentLoadAheadCount(null, null, THREAD_COUNT));
  }

  @Test
  public void test_noWorkerConfig_withContext_usesContext()
  {
    assertEquals(40, DartWorkerContext.resolveSegmentLoadAheadCount(40, null, THREAD_COUNT));
  }

  @Test
  public void test_workerConfig_noContext_usesWorkerConfig()
  {
    assertEquals(128, DartWorkerContext.resolveSegmentLoadAheadCount(null, 128, THREAD_COUNT));
  }

  @Test
  public void test_workerConfig_contextLower_usesWorkerConfigAsFloor()
  {
    assertEquals(128, DartWorkerContext.resolveSegmentLoadAheadCount(64, 128, THREAD_COUNT));
  }

  @Test
  public void test_workerConfig_contextHigher_honorsLargerContext()
  {
    assertEquals(256, DartWorkerContext.resolveSegmentLoadAheadCount(256, 128, THREAD_COUNT));
  }

  @Test
  public void test_workerConfig_contextEqual_returnsThatValue()
  {
    assertEquals(128, DartWorkerContext.resolveSegmentLoadAheadCount(128, 128, THREAD_COUNT));
  }

  @Test
  public void test_nonPositiveWorkerConfig_treatedAsUnset_withContext()
  {
    assertEquals(40, DartWorkerContext.resolveSegmentLoadAheadCount(40, 0, THREAD_COUNT));
    assertEquals(40, DartWorkerContext.resolveSegmentLoadAheadCount(40, -5, THREAD_COUNT));
  }

  @Test
  public void test_nonPositiveWorkerConfig_treatedAsUnset_noContext()
  {
    assertEquals(16, DartWorkerContext.resolveSegmentLoadAheadCount(null, 0, THREAD_COUNT));
    assertEquals(16, DartWorkerContext.resolveSegmentLoadAheadCount(null, -5, THREAD_COUNT));
  }
}
