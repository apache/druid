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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class InputRowFilterResultTest
{
  @Test
  public void testOrdinalValues()
  {
    Assertions.assertEquals(0, InputRowFilterResult.ACCEPTED.ordinal());
    Assertions.assertEquals(1, InputRowFilterResult.NULL_OR_EMPTY_RECORD.ordinal());
    Assertions.assertEquals(2, InputRowFilterResult.BEFORE_MIN_MESSAGE_TIME.ordinal());
    Assertions.assertEquals(3, InputRowFilterResult.AFTER_MAX_MESSAGE_TIME.ordinal());
    Assertions.assertEquals(4, InputRowFilterResult.CUSTOM_FILTER.ordinal());
  }

  @Test
  public void testMetricValues()
  {
    Assertions.assertEquals("accepted", InputRowFilterResult.ACCEPTED.getReason());
    Assertions.assertEquals("null", InputRowFilterResult.NULL_OR_EMPTY_RECORD.getReason());
    Assertions.assertEquals("beforeMinimumMessageTime", InputRowFilterResult.BEFORE_MIN_MESSAGE_TIME.getReason());
    Assertions.assertEquals("afterMaximumMessageTime", InputRowFilterResult.AFTER_MAX_MESSAGE_TIME.getReason());
    Assertions.assertEquals("filtered", InputRowFilterResult.CUSTOM_FILTER.getReason());
  }

  @Test
  public void testEnumCardinality()
  {
    Assertions.assertEquals(6, InputRowFilterResult.values().length);
  }

  @Test
  public void testIsRejected()
  {
    Assertions.assertFalse(InputRowFilterResult.ACCEPTED.isRejected());
    Assertions.assertTrue(InputRowFilterResult.NULL_OR_EMPTY_RECORD.isRejected());
    Assertions.assertTrue(InputRowFilterResult.BEFORE_MIN_MESSAGE_TIME.isRejected());
    Assertions.assertTrue(InputRowFilterResult.AFTER_MAX_MESSAGE_TIME.isRejected());
    Assertions.assertTrue(InputRowFilterResult.CUSTOM_FILTER.isRejected());
    Assertions.assertTrue(InputRowFilterResult.UNKNOWN.isRejected());
  }
}

