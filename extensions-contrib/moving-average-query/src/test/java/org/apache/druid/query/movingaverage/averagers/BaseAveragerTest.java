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

package org.apache.druid.query.movingaverage.averagers;

import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test class for BaseAverager
 */
public class BaseAveragerTest
{

  public static class TestAverager extends BaseAverager<Integer, Integer>
  {
    public TestAverager(Class<Integer> clazz, int b, String name, String field, int cycleSize)
    {
      super(clazz, b, name, field, cycleSize);
    }

    @Override
    protected Integer computeResult()
    {
      return 1;
    }
  }

  @Test
  public void testBaseAverager()
  {
    BaseAverager<Integer, Integer> avg = new TestAverager(Integer.class, 5, "test", "field", 1);

    assertEquals("test", avg.getName());
    assertEquals(5, avg.getNumBuckets());
    assertEquals(5, avg.getBuckets().length);
    assertTrue(avg.getBuckets().getClass().isArray());
  }

  @Test
  public void testAddElement()
  {
    BaseAverager<Integer, Integer> avg = new TestAverager(Integer.class, 3, "test", "field", 1);
    Object[] buckets = avg.getBuckets();

    avg.addElement(Collections.singletonMap("field", 1), Collections.emptyMap());
    assertEquals(Integer.valueOf(1), buckets[0]);
    assertNull(buckets[1]);
    assertNull(buckets[2]);

    avg.addElement(Collections.singletonMap("field", 2), Collections.emptyMap());
    assertEquals(Integer.valueOf(1), buckets[0]);
    assertEquals(Integer.valueOf(2), buckets[1]);
    assertNull(buckets[2]);

    avg.addElement(Collections.singletonMap("field", 3), Collections.emptyMap());
    assertEquals(Integer.valueOf(1), buckets[0]);
    assertEquals(Integer.valueOf(2), buckets[1]);
    assertEquals(Integer.valueOf(3), buckets[2]);

    avg.addElement(Collections.singletonMap("field", 4), Collections.emptyMap());
    assertEquals(Integer.valueOf(4), buckets[0]);
    assertEquals(Integer.valueOf(2), buckets[1]);
    assertEquals(Integer.valueOf(3), buckets[2]);
  }

  @Test
  public void testSkip()
  {
    BaseAverager<Integer, Integer> avg = new TestAverager(Integer.class, 3, "test", "field", 1);
    Object[] buckets = avg.getBuckets();

    avg.addElement(Collections.singletonMap("field", 1), Collections.emptyMap());
    avg.addElement(Collections.singletonMap("field", 1), Collections.emptyMap());
    avg.addElement(Collections.singletonMap("field", 1), Collections.emptyMap());

    assertEquals(Integer.valueOf(1), buckets[0]);
    assertEquals(Integer.valueOf(1), buckets[1]);
    assertEquals(Integer.valueOf(1), buckets[2]);

    avg.skip();
    assertNull(buckets[0]);
    assertNotNull(buckets[1]);
    assertNotNull(buckets[2]);

    avg.skip();
    assertNull(buckets[0]);
    assertNull(buckets[1]);
    assertNotNull(buckets[2]);

    avg.skip();
    assertNull(buckets[0]);
    assertNull(buckets[1]);
    assertNull(buckets[2]);

    // poke some test data into the array
    buckets[0] = Integer.valueOf(1);

    avg.skip();
    assertNull(buckets[0]);
    assertNull(buckets[1]);
    assertNull(buckets[2]);
  }

  @Test
  public void testHasData()
  {
    BaseAverager<Integer, Integer> avg = new TestAverager(Integer.class, 3, "test", "field", 1);

    assertFalse(avg.hasData());

    avg.addElement(Collections.singletonMap("field", 1), Collections.emptyMap());
    assertTrue(avg.hasData());

    avg.skip();
    avg.skip();
    avg.skip();

    assertFalse(avg.hasData());
  }

  @Test
  public void testGetResult()
  {
    BaseAverager<Integer, Integer> avg = new TestAverager(Integer.class, 3, "test", "field", 1);

    assertNull(avg.getResult());

    avg.addElement(Collections.singletonMap("field", 1), Collections.emptyMap());
    assertEquals(Integer.valueOf(1), avg.getResult());
  }

}
