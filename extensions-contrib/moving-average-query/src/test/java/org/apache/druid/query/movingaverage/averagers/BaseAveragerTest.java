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

import org.junit.Assert;
import org.junit.Test;

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

    Assert.assertEquals("test", avg.getName());
    Assert.assertEquals(5, avg.getNumBuckets());
    Assert.assertEquals(5, avg.getBuckets().length);
    Assert.assertTrue(avg.getBuckets().getClass().isArray());
  }

  @Test
  public void testAddElement()
  {
    BaseAverager<Integer, Integer> avg = new TestAverager(Integer.class, 3, "test", "field", 1);
    Object[] buckets = avg.getBuckets();

    avg.addElement(Collections.singletonMap("field", 1), Collections.emptyMap());
    Assert.assertEquals(1, buckets[0]);
    Assert.assertNull(buckets[1]);
    Assert.assertNull(buckets[2]);

    avg.addElement(Collections.singletonMap("field", 2), Collections.emptyMap());
    Assert.assertEquals(1, buckets[0]);
    Assert.assertEquals(2, buckets[1]);
    Assert.assertNull(buckets[2]);

    avg.addElement(Collections.singletonMap("field", 3), Collections.emptyMap());
    Assert.assertEquals(1, buckets[0]);
    Assert.assertEquals(2, buckets[1]);
    Assert.assertEquals(3, buckets[2]);

    avg.addElement(Collections.singletonMap("field", 4), Collections.emptyMap());
    Assert.assertEquals(4, buckets[0]);
    Assert.assertEquals(2, buckets[1]);
    Assert.assertEquals(3, buckets[2]);
  }

  @Test
  public void testSkip()
  {
    BaseAverager<Integer, Integer> avg = new TestAverager(Integer.class, 3, "test", "field", 1);
    Object[] buckets = avg.getBuckets();

    avg.addElement(Collections.singletonMap("field", 1), Collections.emptyMap());
    avg.addElement(Collections.singletonMap("field", 1), Collections.emptyMap());
    avg.addElement(Collections.singletonMap("field", 1), Collections.emptyMap());

    Assert.assertEquals(1, buckets[0]);
    Assert.assertEquals(1, buckets[1]);
    Assert.assertEquals(1, buckets[2]);

    avg.skip();
    Assert.assertNull(buckets[0]);
    Assert.assertNotNull(buckets[1]);
    Assert.assertNotNull(buckets[2]);

    avg.skip();
    Assert.assertNull(buckets[0]);
    Assert.assertNull(buckets[1]);
    Assert.assertNotNull(buckets[2]);

    avg.skip();
    Assert.assertNull(buckets[0]);
    Assert.assertNull(buckets[1]);
    Assert.assertNull(buckets[2]);

    // poke some test data into the array
    buckets[0] = 1;

    avg.skip();
    Assert.assertNull(buckets[0]);
    Assert.assertNull(buckets[1]);
    Assert.assertNull(buckets[2]);
  }

  @Test
  public void testHasData()
  {
    BaseAverager<Integer, Integer> avg = new TestAverager(Integer.class, 3, "test", "field", 1);

    Assert.assertFalse(avg.hasData());

    avg.addElement(Collections.singletonMap("field", 1), Collections.emptyMap());
    Assert.assertTrue(avg.hasData());

    avg.skip();
    avg.skip();
    avg.skip();

    Assert.assertFalse(avg.hasData());
  }

  @Test
  public void testGetResult()
  {
    BaseAverager<Integer, Integer> avg = new TestAverager(Integer.class, 3, "test", "field", 1);

    Assert.assertNull(avg.getResult());

    avg.addElement(Collections.singletonMap("field", 1), Collections.emptyMap());
    Assert.assertEquals(Integer.valueOf(1), avg.getResult());
  }
}
