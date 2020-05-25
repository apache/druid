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

package org.apache.druid.query.aggregation.histogram;

import org.junit.Assert;
import org.junit.Test;

public class ArrayUtilsTest
{
  @Test
  public void testHashCodeLong()
  {
    int hash1 = ArrayUtils.hashCode(new long[]{1L, 2L, 3L}, 0, 2);
    int hash2 = ArrayUtils.hashCode(new long[]{1L, 2L, 3L}, 0, 2);
    int hash3 = ArrayUtils.hashCode(new long[]{1L, 2L, 3L}, 0, 1);
    int hash4 = ArrayUtils.hashCode(new long[]{1L, 2L, 3L}, 0, 1);

    Assert.assertEquals(hash1, hash2);
    Assert.assertNotEquals(hash1, hash3);
    Assert.assertEquals(hash3, hash4);
  }

  @Test
  public void testHashCodeFloat()
  {
    int hash1 = ArrayUtils.hashCode(new float[]{1.0f, 2.0f, 3.0f}, 0, 2);
    int hash2 = ArrayUtils.hashCode(new float[]{1.0f, 2.0f, 3.0f}, 0, 2);
    int hash3 = ArrayUtils.hashCode(new float[]{1.0f, 2.0f, 3.0f}, 0, 1);
    int hash4 = ArrayUtils.hashCode(new float[]{1.0f, 2.0f, 3.0f}, 0, 1);

    Assert.assertEquals(hash1, hash2);
    Assert.assertNotEquals(hash1, hash3);
    Assert.assertEquals(hash3, hash4);
  }

  @Test
  public void testHashCodeDouble()
  {
    int hash1 = ArrayUtils.hashCode(new double[]{1.0, 2.0, 3.0}, 0, 2);
    int hash2 = ArrayUtils.hashCode(new double[]{1.0, 2.0, 3.0}, 0, 2);
    int hash3 = ArrayUtils.hashCode(new double[]{1.0, 2.0, 3.0}, 0, 1);
    int hash4 = ArrayUtils.hashCode(new double[]{1.0, 2.0, 3.0}, 0, 1);

    Assert.assertEquals(hash1, hash2);
    Assert.assertNotEquals(hash1, hash3);
    Assert.assertEquals(hash3, hash4);
  }
}
