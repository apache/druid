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
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OffsetLimitTest
{
  @Test
  public void testNone()
  {
    assertFalse(OffsetLimit.NONE.isPresent());
    assertFalse(OffsetLimit.NONE.hasOffset());
    assertFalse(OffsetLimit.NONE.hasLimit());
  }

  @Test
  public void testOffset()
  {
    int offset = 3;
    OffsetLimit ol = new OffsetLimit(offset, -1);
    assertTrue(ol.hasOffset());
    assertFalse(ol.hasLimit());
    assertEquals(offset, ol.getOffset());
    assertEquals(-1, ol.getLimit());
    assertEquals(Long.MAX_VALUE, ol.getLimitOrMax());
    assertEquals(offset, ol.getFromIndex(Long.MAX_VALUE));
    assertEquals(Long.MAX_VALUE, ol.getToIndex(Long.MAX_VALUE));
    assertEquals(0, ol.getFromIndex(1));
    assertEquals(0, ol.getFromIndex(offset));
    assertEquals(0, ol.getToIndex(offset));
  }

  @Test
  public void testLimit()
  {
    OffsetLimit ol = new OffsetLimit(0, 4);
    assertFalse(ol.hasOffset());
    assertTrue(ol.hasLimit());
    assertEquals(0, ol.getOffset());
    assertEquals(4, ol.getLimit());
    assertEquals(4, ol.getLimitOrMax());
    assertEquals(0, ol.getFromIndex(Long.MAX_VALUE));
    assertEquals(4, ol.getToIndex(Long.MAX_VALUE));
    assertEquals(0, ol.getFromIndex(2));
    assertEquals(2, ol.getToIndex(2));
  }

  @Test
  public void testOffsetLimit()
  {
    int offset = 3;
    int limit = 10;
    OffsetLimit ol = new OffsetLimit(offset, limit);
    assertTrue(ol.hasOffset());
    assertTrue(ol.hasLimit());
    assertEquals(offset, ol.getOffset());
    assertEquals(limit, ol.getLimit());
    assertEquals(limit, ol.getLimitOrMax());
    assertEquals(offset, ol.getFromIndex(Long.MAX_VALUE));
    assertEquals(offset + limit, ol.getToIndex(Long.MAX_VALUE));
    assertEquals(0, ol.getFromIndex(offset));
    assertEquals(0, ol.getToIndex(offset));
    assertEquals(offset, ol.getFromIndex(offset + 1));
    assertEquals(offset + 1, ol.getToIndex(offset + 1));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidOffset()
  {
    new OffsetLimit(-1, -1);
  }

  @Test
  public void testNegativeLimitsAreNotDifferent()
  {
    OffsetLimit ol1 = new OffsetLimit(1, -1);
    OffsetLimit ol2 = new OffsetLimit(1, -2);
    assertEquals(ol1, ol2);
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(OffsetLimit.class).verify();
  }
}
