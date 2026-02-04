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

package org.apache.druid.server.compaction;

import org.junit.Assert;
import org.junit.Test;

public class CompactionEligibilityTest
{
  @Test
  public void testEqualsAndHashCode()
  {
    final CompactionEligibility e1 = CompactionEligibility.fail("reason");
    final CompactionEligibility e2 = CompactionEligibility.fail("reason");
    final CompactionEligibility e3 = CompactionEligibility.fail("different");
    final CompactionEligibility e4 = CompactionEligibility.incrementalCompaction("reason");

    Assert.assertEquals(e1, e2);
    Assert.assertEquals(e1.hashCode(), e2.hashCode());
    Assert.assertNotEquals(e1, e3);
    Assert.assertNotEquals(e1, e4);
  }
}
