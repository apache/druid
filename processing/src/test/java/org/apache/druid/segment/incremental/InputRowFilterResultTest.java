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

public class InputRowFilterResultTest
{
  @Test
  public void testOrdinalValues()
  {
    Assert.assertEquals(0, InputRowFilterResult.ACCEPTED.ordinal());
    Assert.assertEquals(1, InputRowFilterResult.NULL_OR_EMPTY_RECORD.ordinal());
    Assert.assertEquals(2, InputRowFilterResult.BEFORE_MIN_MESSAGE_TIME.ordinal());
    Assert.assertEquals(3, InputRowFilterResult.AFTER_MAX_MESSAGE_TIME.ordinal());
    Assert.assertEquals(4, InputRowFilterResult.FILTERED.ordinal());
  }

  @Test
  public void testMetricValues()
  {
    Assert.assertEquals("accepted", InputRowFilterResult.ACCEPTED.getReason());
    Assert.assertEquals("null", InputRowFilterResult.NULL_OR_EMPTY_RECORD.getReason());
    Assert.assertEquals("beforeMinMessageTime", InputRowFilterResult.BEFORE_MIN_MESSAGE_TIME.getReason());
    Assert.assertEquals("afterMaxMessageTime", InputRowFilterResult.AFTER_MAX_MESSAGE_TIME.getReason());
    Assert.assertEquals("filtered", InputRowFilterResult.FILTERED.getReason());
  }

  @Test
  public void testEnumCardinality()
  {
    Assert.assertEquals(6, InputRowFilterResult.values().length);
  }

  @Test
  public void testIsRejected()
  {
    Assert.assertFalse(InputRowFilterResult.ACCEPTED.isRejected());
    Assert.assertTrue(InputRowFilterResult.NULL_OR_EMPTY_RECORD.isRejected());
    Assert.assertTrue(InputRowFilterResult.BEFORE_MIN_MESSAGE_TIME.isRejected());
    Assert.assertTrue(InputRowFilterResult.AFTER_MAX_MESSAGE_TIME.isRejected());
    Assert.assertTrue(InputRowFilterResult.FILTERED.isRejected());
    Assert.assertTrue(InputRowFilterResult.UNKNOWN.isRejected());
  }

  @Test
  public void testBuildRejectedCounterMapExcludesAccepted()
  {
    var counterMap = InputRowFilterResult.buildRejectedCounterMap();
    Assert.assertFalse(counterMap.containsKey(InputRowFilterResult.ACCEPTED.getReason()));
    Assert.assertTrue(counterMap.containsKey(InputRowFilterResult.NULL_OR_EMPTY_RECORD.getReason()));
    Assert.assertTrue(counterMap.containsKey(InputRowFilterResult.BEFORE_MIN_MESSAGE_TIME.getReason()));
    Assert.assertTrue(counterMap.containsKey(InputRowFilterResult.AFTER_MAX_MESSAGE_TIME.getReason()));
    Assert.assertTrue(counterMap.containsKey(InputRowFilterResult.FILTERED.getReason()));
    Assert.assertTrue(counterMap.containsKey(InputRowFilterResult.UNKNOWN.getReason()));
  }
}

