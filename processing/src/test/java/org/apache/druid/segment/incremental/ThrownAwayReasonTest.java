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

package org.apache.druid.segment.incremental;

import org.junit.Assert;
import org.junit.Test;

public class ThrownAwayReasonTest
{
  @Test
  public void testOrdinalValues()
  {
    Assert.assertEquals(0, ThrownAwayReason.NULL.ordinal());
    Assert.assertEquals(1, ThrownAwayReason.BEFORE_MIN_MESSAGE_TIME.ordinal());
    Assert.assertEquals(2, ThrownAwayReason.AFTER_MAX_MESSAGE_TIME.ordinal());
    Assert.assertEquals(3, ThrownAwayReason.FILTERED.ordinal());
  }

  @Test
  public void testMetricValues()
  {
    Assert.assertEquals("null", ThrownAwayReason.NULL.getMetricValue());
    Assert.assertEquals("beforeMinMessageTime", ThrownAwayReason.BEFORE_MIN_MESSAGE_TIME.getMetricValue());
    Assert.assertEquals("afterMaxMessageTime", ThrownAwayReason.AFTER_MAX_MESSAGE_TIME.getMetricValue());
    Assert.assertEquals("filtered", ThrownAwayReason.FILTERED.getMetricValue());
  }

  @Test
  public void testEnumCount()
  {
    Assert.assertEquals(4, ThrownAwayReason.values().length);
  }
}

