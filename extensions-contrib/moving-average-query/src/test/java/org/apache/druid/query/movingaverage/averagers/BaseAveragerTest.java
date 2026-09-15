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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class BaseAveragerTest
{

  public static class TestAverager extends BaseAverager<Integer, Integer>
  {
    TestAverager(Class<Integer> clazz, int b, String name, String field, int cycleSize)
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

    Assertions.assertEquals("test", avg.getName());
    Assertions.assertEquals(5, avg.getNumBuckets());
    Assertions.assertEquals(5, avg.getBuckets().length);
    Assertions.assertTrue(avg.getBuckets().getClass().isArray());
  }

  @Test
  public void testAddElement()
  {
    BaseAverager<Integer, Integer> avg = new TestAverager(Integer.class, 3, "test", "field", 1);
    Object[] buckets = avg.getBuckets();

    avg.addElement(Collections.singletonMap("field", 1), Collections.emptyMap());
    Assertions.assertEquals(1, buckets[0]);
    Assertions.assertNull(buckets[1]);
    Assertions.assertNull(buckets[2]);

    avg.addElement(Collections.singletonMap("field", 2), Collections.emptyMap());
    Assertions.assertEquals(1, buckets[0]);
    Assertions.assertEquals(2, buckets[1]);
    Assertions.assertNull(buckets[2]);

    avg.addElement(Collections.singletonMap("field", 3), Collections.emptyMap());
    Assertions.assertEquals(1, buckets[0]);
    Assertions.assertEquals(2, buckets[1]);
    Assertions.assertEquals(3, buckets[2]);

    avg.addElement(Collections.singletonMap("field", 4), Collections.emptyMap());
    Assertions.assertEquals(4, buckets[0]);
    Assertions.assertEquals(2, buckets[1]);
    Assertions.assertEquals(3, buckets[2]);
  }

  @Test
  public void testSkip()
  {
    BaseAverager<Integer, Integer> avg = new TestAverager(Integer.class, 3, "test", "field", 1);
    Object[] buckets = avg.getBuckets();

    avg.addElement(Collections.singletonMap("field", 1), Collections.emptyMap());
    avg.addElement(Collections.singletonMap("field", 1), Collections.emptyMap());
    avg.addElement(Collections.singletonMap("field", 1), Collections.emptyMap());

    Assertions.assertEquals(1, buckets[0]);
    Assertions.assertEquals(1, buckets[1]);
    Assertions.assertEquals(1, buckets[2]);

    avg.skip();
    Assertions.assertNull(buckets[0]);
    Assertions.assertNotNull(buckets[1]);
    Assertions.assertNotNull(buckets[2]);

    avg.skip();
    Assertions.assertNull(buckets[0]);
    Assertions.assertNull(buckets[1]);
    Assertions.assertNotNull(buckets[2]);

    avg.skip();
    Assertions.assertNull(buckets[0]);
    Assertions.assertNull(buckets[1]);
    Assertions.assertNull(buckets[2]);

    // poke some test data into the array
    buckets[0] = 1;

    avg.skip();
    Assertions.assertNull(buckets[0]);
    Assertions.assertNull(buckets[1]);
    Assertions.assertNull(buckets[2]);
  }

  @Test
  public void testHasData()
  {
    BaseAverager<Integer, Integer> avg = new TestAverager(Integer.class, 3, "test", "field", 1);

    Assertions.assertFalse(avg.hasData());

    avg.addElement(Collections.singletonMap("field", 1), Collections.emptyMap());
    Assertions.assertTrue(avg.hasData());

    avg.skip();
    avg.skip();
    avg.skip();

    Assertions.assertFalse(avg.hasData());
  }

  @Test
  public void testGetResult()
  {
    BaseAverager<Integer, Integer> avg = new TestAverager(Integer.class, 3, "test", "field", 1);

    Assertions.assertNull(avg.getResult());

    avg.addElement(Collections.singletonMap("field", 1), Collections.emptyMap());
    Assertions.assertEquals(Integer.valueOf(1), avg.getResult());
  }
}
