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

package org.apache.druid.query.operator;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OffsetLimitTest
{
  @Test
  public void testNone()
  {
    Assertions.assertFalse(OffsetLimit.NONE.isPresent());
    Assertions.assertFalse(OffsetLimit.NONE.hasOffset());
    Assertions.assertFalse(OffsetLimit.NONE.hasLimit());
  }

  @Test
  public void testOffset()
  {
    int offset = 3;
    OffsetLimit ol = new OffsetLimit(offset, -1);
    Assertions.assertTrue(ol.hasOffset());
    Assertions.assertFalse(ol.hasLimit());
    Assertions.assertEquals(offset, ol.getOffset());
    Assertions.assertEquals(-1, ol.getLimit());
    Assertions.assertEquals(Long.MAX_VALUE, ol.getLimitOrMax());
    Assertions.assertEquals(offset, ol.getFromIndex(Long.MAX_VALUE));
    Assertions.assertEquals(Long.MAX_VALUE, ol.getToIndex(Long.MAX_VALUE));
    Assertions.assertEquals(0, ol.getFromIndex(1));
    Assertions.assertEquals(0, ol.getFromIndex(offset));
    Assertions.assertEquals(0, ol.getToIndex(offset));
  }

  @Test
  public void testLimit()
  {
    OffsetLimit ol = new OffsetLimit(0, 4);
    Assertions.assertFalse(ol.hasOffset());
    Assertions.assertTrue(ol.hasLimit());
    Assertions.assertEquals(0, ol.getOffset());
    Assertions.assertEquals(4, ol.getLimit());
    Assertions.assertEquals(4, ol.getLimitOrMax());
    Assertions.assertEquals(0, ol.getFromIndex(Long.MAX_VALUE));
    Assertions.assertEquals(4, ol.getToIndex(Long.MAX_VALUE));
    Assertions.assertEquals(0, ol.getFromIndex(2));
    Assertions.assertEquals(2, ol.getToIndex(2));
  }

  @Test
  public void testOffsetLimit()
  {
    int offset = 3;
    int limit = 10;
    OffsetLimit ol = new OffsetLimit(offset, limit);
    Assertions.assertTrue(ol.hasOffset());
    Assertions.assertTrue(ol.hasLimit());
    Assertions.assertEquals(offset, ol.getOffset());
    Assertions.assertEquals(limit, ol.getLimit());
    Assertions.assertEquals(limit, ol.getLimitOrMax());
    Assertions.assertEquals(offset, ol.getFromIndex(Long.MAX_VALUE));
    Assertions.assertEquals(offset + limit, ol.getToIndex(Long.MAX_VALUE));
    Assertions.assertEquals(0, ol.getFromIndex(offset));
    Assertions.assertEquals(0, ol.getToIndex(offset));
    Assertions.assertEquals(offset, ol.getFromIndex(offset + 1));
    Assertions.assertEquals(offset + 1, ol.getToIndex(offset + 1));
  }

  @Test
  public void testInvalidOffset()
  {
    Assertions.assertThrows(IllegalArgumentException.class, () -> new OffsetLimit(-1, -1));
  }

  @Test
  public void testNegativeLimitsAreNotDifferent()
  {
    OffsetLimit ol1 = new OffsetLimit(1, -1);
    OffsetLimit ol2 = new OffsetLimit(1, -2);
    Assertions.assertEquals(ol1, ol2);
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(OffsetLimit.class).verify();
  }
}
