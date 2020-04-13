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

package org.apache.druid.segment.data;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 */
@RunWith(Parameterized.class)
public class IndexedIntsTest
{
  private static final int[] ARRAY = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

  private final IndexedInts indexed;

  @Parameterized.Parameters
  public static Collection<?> constructorFeeder()
  {
    return Arrays.asList(
        new Object[][]{
            {VSizeColumnarInts.fromArray(ARRAY)},
            {new ArrayBasedIndexedInts(ARRAY)}
        }
    );
  }

  public IndexedIntsTest(
      IndexedInts indexed
  )
  {
    this.indexed = indexed;
  }

  @Test
  public void testSanity()
  {
    Assert.assertEquals(ARRAY.length, indexed.size());
    for (int i = 0; i < ARRAY.length; i++) {
      Assert.assertEquals(ARRAY[i], indexed.get(i));
    }
  }
}
