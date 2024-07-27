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

package org.apache.druid.indexing.compact;

import org.junit.Test;

/**
 * Tests the scheduling behaviour of the Compaction Scheduler and not the
 * compaction of segments itself. There are other tests which already verify
 * the compaction of segments with different configs and datasources.
 */
public class CompactionSchedulerTest
{
  // what are the different aspects we would like to test
  // task status updates
  //
  // config
  //

  // there should also be a test where we can do some sort of comparison of the two things

  // CompactSegmentsTest is not the right place for that because the entry point is CompactSegments

  // For us, the entry point is DruidCoordinator (i.e. sim) vs Compaction Scheduler.

  @Test
  public void test()
  {

  }

}
